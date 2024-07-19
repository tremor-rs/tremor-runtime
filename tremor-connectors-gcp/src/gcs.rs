// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::doc_markdown)]

//! ## `gcs_streamer`
//!
//! This connector provides the ability to stream events into Google Cloud Storage.
//!
//! :::note
//!    Authentication happens over the [GCP autentication](./index.md#GCP)
//! :::
//!
//! For this connector, while the (name, bucket name) pair does not change, the consecutive events will be appended to the same gcs object.
//!
//! ### Modes of operation
//!
//! All our object storage connectors that do uploads ([`s3_streamer`] and `gcs_streamer`) operate with the same two modes.
//!
//! #### `yolo`
//!
//! The `yolo` mode is going to report every event as successfully delivered if uploading it to gcs worked or not.
//! This mode is intended for "fire-and-forget" use-cases, where it is not important if a single event in the middle of an upload failed or is missing.
//! The upload will continue nonetheless and only be finished when a new event with another bucket or name is received.
//!
//! This mode is well-suited for e.g. aggregating time-series events into gcs object by time unit (e.g. hour or day) where it is not too important that the resulting gcs object is 1:1 mirroring of all the events in the correct order without gaps.
//!
//! #### `consistent`
//!
//! `consistent` mode, on the other hand, is well suited when uploading consistent and correct gcs objects is important. In this mode, the `gcs_streamer` will fail a complete upload whenever only 1 single event could not be successfully uploaded to gcs. This guarantees that only complete and consistent gcs objects will show up in your buckets. But due to this logic, it is possible to lose events and whole uploads if the upstream connectors don't replay failed events. The [`wal` connector](./wal.md) and the [`kafka_consumer`](./kafka.md#consumer) can replay events, so it makes sense to pair the `gcs_streamer` in `consistent` mode with those.
//!
//! This mode guarantees that only complete gcs objects with all events in the right order are uploaded to gcs, object where uploading failed for some reason will be deleted, expecting the upstream to retry the upload. It is well-suited for uploading complete files, like images or documents.
//!
//! ### Metadata
//! Two metadata fields are required for the connector to work - `$gcs_streamer.name` (will be used as the object name) and `$gcs_streamer.bucket` (the name of the bucket where the object will be placed).
//!
//! ### Configuration
//!
//! All of the configuration options,, aside of token, are optional.
//!
//! | name                | description                                                                                            | default                                               |
//! |---------------------|--------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
//! | `url`               | The HTTP(s) endpoint to which the requests will be made                                                | `"https://storage.googleapis.com/upload/storage/v1/"` |
//! | `bucket`            | The optional bucket to stream events into if not overwritten by event metadata `$gcs_streamer.bucket`  |                                                       |
//! | `mode`              | The mode of operation for this connector. See [Modes of operation](#modes-of-operation).               |                                                       |
//! | `connect_timeout`   | The timeout for the connection (in nanoseconds)                                                        | `10_000_000_000` (10 seconds)                         |
//! | `buffer_size`       | The size of a single request body, in bytes (must be divisible by 256kiB, as required by Google)       | `8388608` (8MiB, the minimum recommended by Google)   |
//! | `max_retries`       | The number of retries to perform for a failed request to GCS.                                          | `3`                                                   |
//! | `backoff_base_time` | Base waiting time in nanoseconds for exponential backoff when doing retries of failed requests to GCS. | `25_000_000` (25 ms)                                  |
//! | `token`             | The authentication token see [GCP autentication](./index.md#GCP)                                       |                                                       |
//!
//! ### Example
//!
//! ```tremor title="config.troy"
//! define flow main
//! flow
//!     use std::{size, time::nanos};
//!
//!     define connector metronome from metronome
//!     with
//!         config = {"interval": nanos::from_millis(10)}
//!     end;
//!
//!     define connector output from gcs_streamer
//!     with
//!         config = {
//!             "mode": "consistent",
//!             "buffer_size": size::kiB(64),
//!             "backoff_base_time": nanos::from_millis(200)
//!         },
//!         codec = "json"
//!     end;
//!
//!     define pipeline main
//!     pipeline
//!         define script add_meta
//!         script
//!             use std::string;
//!
//!             let file_id = event.id - (event.id % 4);
//!
//!             let $gcs_streamer = {
//!                 "name": "my_file_#{"#{file_id}"}.txt",
//!                 "bucket": "tremor-test-bucket"
//!             };
//!
//!             emit {"a": "B"}
//!         end;
//!
//!         create script add_meta from add_meta;
//!
//!         select event from in into add_meta;
//!         select event from add_meta into out;
//!         select event from add_meta/err into err;
//!     end;
//!
//!     define connector console from stdio
//!     with
//!         codec = "json"
//!     end;
//!
//!     create connector s1 from metronome;
//!     create connector s2 from output;
//!     create connector errors from console;
//!
//!     create pipeline main;
//!
//!     connect /connector/s1 to /pipeline/main;
//!     connect /pipeline/main to /connector/s2;
//!     connect /pipeline/main/err to /connector/errors;
//! end;
//!
//! deploy flow main;
//! ```
//!
//! [`s3_streamer`]: ./s3.md#s3-streamer

use http::StatusCode;

mod chunked_buffer;
mod resumable_upload_client;
pub(crate) mod streamer;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("No client available")]
    NoClient,
    #[error("Buffer was marked as done at index {0} which is not in memory anymore")]
    InvalidBuffer(usize),
    #[error("Not enough data in the buffer")]
    NotEnoughData,
    #[error("Request still failing after {0} retries")]
    RequestFailed(u32),
    #[error("Request for bucket {0} failed with {1}")]
    Bucket(String, StatusCode),
    #[error("Upload failed with {0}")]
    Upload(StatusCode),

    #[error("Delete failed with {0}")]
    Delete(StatusCode),
    #[error("Missing location header")]
    MissingLocationHeader,
    #[error("Missing or invalid range header")]
    MissingOrInvalidRangeHeader,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn error_to_string_mapping() {
        assert_eq!("No client available", Error::NoClient.to_string());
        assert_eq!(
            "Buffer was marked as done at index 666 which is not in memory anymore",
            Error::InvalidBuffer(666).to_string()
        );
        assert_eq!(
            "Not enough data in the buffer",
            Error::NotEnoughData.to_string()
        );
        assert_eq!(
            "Request still failing after 3 retries",
            Error::RequestFailed(3).to_string()
        );
        assert_eq!(
            "Request for bucket snot failed with 404 Not Found",
            Error::Bucket("snot".to_string(), StatusCode::NOT_FOUND).to_string()
        );
        assert_eq!(
            "Upload failed with 403 Forbidden",
            Error::Upload(StatusCode::FORBIDDEN).to_string()
        );
        assert_eq!(
            "Delete failed with 404 Not Found",
            Error::Delete(StatusCode::NOT_FOUND).to_string()
        );
        assert_eq!(
            "Missing location header",
            Error::MissingLocationHeader.to_string()
        );
        assert_eq!(
            "Missing or invalid range header",
            Error::MissingOrInvalidRangeHeader.to_string()
        );
    }
}
