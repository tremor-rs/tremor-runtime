// Copyright 2022, The Tremor Team
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
//! The Amazon Web Services `s3` provides integration with the AWS Simple Storage Service or
//! drop-in compatible replacements such as [MinIO](https://www.min.io).
//!
//! Both the [`s3_reader`](#s3reader) and the [`s3_streamer`](#s3streamer) need to authenticate to AWS. This connector uses the default ways of obtaining credentials
//! from common environment variables like `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, from the AWS `credentials` file or from EC2 instance metadata. Please visit [The AWS docs](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html) for more infos on how to provide your credentials.
//!
//! ## `s3_reader`
//!
//! This connector reads objects from an S3 bucket with the given `prefix`, or all object in that bucket if no prefix was provided.
//! It does not listen for object changes or new objects being uploaded. It traverses through the bucket once and starts fetching with `max_connections` concurrent tasks.
//!
//! This connector uses multipart downloads if an object size exceeds the configurable `multipart_threshold`.
//!
//! ### Configuration
//!
//! | Option                | Description                                                                                                                                                                     | Type             | Required | Default Value |
//! |-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|----------|---------------|
//! | `aws_region`          | Name of the AWS region the bucket is in                                                                                                                                         | string           | yes      |               |
//! | `url`                 | Endpoint / URL to connect to. Useful for connecting to local or non-AWS systems.                                                                                                | URL              | no       |               |
//! | `bucket`              | Name of the bucket to read objects from                                                                                                                                         | string           | yes      |               |
//! | `prefix`              | Optional prefix. This connector will only fetch objects whose key starts with the given prefix. If no prefix is provided, all objects will be fetched.                          | string           | no       |               |
//! | `multipart_threshold` | Threshold in bytes that, if an object exceeds this size, it is fetched using multipart download.                                                                                | positive integer | no       | 8388608 (8MB) |
//! | `multipart_chunksize` | The size of a single multipart chunk when using multipart download.                                                                                                             | positive integer | no       | 8388608 (8MB) |
//! | `max_connections`     | The maximum number of concurrent connections to keep open. This roughly translates to the number of parallel downloads to use                                                   | positive integer | no       | 10            |
//! | `path_style_access`   | If set to `false`, the connector will access the bucket via subdomain (e.g. `mybucket.s3.amazonaws.com`). Set this to `true` if accessing s3-compatible stores like e.g. minio. | boolean          | no       | `true`        |
//!
//! Example:
//!
//! ```tremor title="config.troy"
//! use std::size;
//! define connector s3_reader from s3_reader
//! with
//!   codec = "json",
//!   preprocessors = ["separate"],
//!   postprocessors = ["separate"],
//!   config = {
//!     "aws_region":  "eu-central-1",
//!     "url": "localhost:9000",
//!     "bucket": "my_bucket",
//!     "prefix": "prefix/",
//!
//!     # only use multipart when file > 16MB
//!     multipart_threshold: size::MiB(16),
//!
//!     # restrict connector to 1 concurrent download
//!     max_connections: 1,
//!   },
//! end;
//! ```
//!
//! ## `s3_streamer`
//!
//! This connector will write events to the configured AWS S3 bucket.
//!
//! The event needs to carry the object name this event should be written to in its metadata.
//! It can carry the bucket name if it differs from the one configured in the connector or if none has been configured.
//!
//! ```js
//! {
//!   "s3_streamer": {
//!     "name": "my_object_key",
//!     "bucket": "my_bucket"
//!   }
//! }
//! ```
//!
//! Events will be uploaded using a multipart upload. Multiple event payloads will be appended to the same object if their `$s3_streamer.name` and `$s3_streamer.bucket` metadata is the same.
//! Once this connector receives an event with a different name and bucket metadata, the previous upload will be completed and a new one will be started.
//!
//! This connector will only append event payloads to the same object if the name and bucket metadata of the previous event was the same. If not, it will start a new upload, possibly overwriting
//! an object previously stored. Care should be taken to have events come with stable keys, not changing forth and back in between.
//!
//! This connector was built with the use case of time based aggregation in mind. E.g. events are aggregated into s3 objects with 1 object containing all objects that arrived within 1 hour. See the example below.
//!
//!
//! ### Modes of operation
//!
//! The `s3_streamer` operates in two different modes. The default mode is `yolo`:
//!
//! #### `yolo`
//!
//! The `yolo` mode is going to report every event as successfully delivered if uploading it to s3 worked or not.
//! This mode is intended for "fire-and-forget" use-cases, where it is not important if a single event in the middle of an upload failed or is missing.
//! The upload will continue nonetheless and only be finished when a new event with another bucket or name is received.
//!
//! This mode is well-suited for e.g. aggregating time-series events into s3 object by time unit (e.g. hour or day) where it is not too important that the resulting s3 object is 1:1 mirroring of all the events in the correct order without gaps.
//!
//! #### `consistent`
//!
//! `consistent` mode, on the other hand, is well suited when uploading consistent and correct s3 objects is important. In this mode, the `s3_streamer` will fail a complete upload whenever only 1 single event could not be successfully uploaded to s3. This guarantees that only complete and consistent s3 objects will show up in your buckets. But due to this logic, it is possible to lose events and whole uploads if the upstream connectors don't replay failed events. The [`wal` connector](./wal.md) and the [`kafka_consumer`](./kafka.md#consumer) can replay events, so it makes sense to pair the `s3_streamer` in `consistent` mode with those.
//!
//! This mode guarantees that only complete s3 objects with all events in the right order are uploaded to s3. It is well-suited for uploading complete files, like images or documents.
//!
//!
//! ### Configuration
//!
//! | Option              | Description                                                                                                                                                                      | Type                   | Required | Default Value             |
//! |---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------|----------|---------------------------|
//! | `mode`              | [Mode of operation](#modes-of-operation)                                                                                                                                         | `yolo` or `consistent` | no       | `yolo`                    |
//! | `aws_region`        | Name of the AWS region the bucket is in                                                                                                                                          | string                 | yes      |                           |
//! | `url`               | Endpoint / URL to connect to. Useful for connecting to local or non-AWS systems.                                                                                                 | URL                    | no       |                           |
//! | `bucket`            | Name of the bucket to stream objects to                                                                                                                                          | string                 | yes      |                           |
//! | `buffer_size`       | Minimum chunk size in bytes for multipart uploads. AWS S3 demands this to be greater than 5MB.                                                                                   | positive integer       | no       | 5242980 (5MB + 100 Bytes) |
//! | `path_style_access` | If set to `false`, the connector will access the bucket via subdomain (e.g. `mybucket.s3.amazonaws.com`). Set this to `true` for accessing s3-compatible stores like e.g. minio. | boolean                | no       | `true`                    |
//!
//!
//! Example:
//!
//! ```tremor title="config.troy"
//!   use std::size;
//!   define connector s3_streamer from s3_streamer
//!   with
//!     codec = "json",
//!     preprocessors = ["separate"],
//!     postprocessors = ["separate"],
//!     config = {
//!       "aws_region":  "eu-central-1",
//!       "url": "localhost:9000",
//!       "bucket": "snot",
//!       "mode": "yolo",
//!       # increase minimum part size from 5 to 10MB
//!       "min_part_size": size::MiB(10)
//!     },
//!   end;
//! ```
//!
//! :::note
//!    Environment variables should be set according to the AWS [environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html).
//!
//!    A bucket called `snot` will need to be available.
//!
//!    The region in Minio settings should be the same as configured in the server startup.
//! :::

mod auth;
pub(crate) mod reader;
pub(crate) mod streamer;

#[derive(Clone, thiserror::Error, Debug)]
enum Error {
    #[error("Source sender not initialized")]
    NoSource,
    #[error("No s3 client available")]
    NoClient,
    #[error("Failed to start upload for `{0}`")]
    UploadStart(String),
}
