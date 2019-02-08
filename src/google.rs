// Copyright 2018-2019, Wayfair GmbH
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

use google_pubsub1 as pubsub;
use google_storage1::{Error, Storage};
use hyper::net::HttpsConnector;
use log::Level;
use std::io::Result;
use yup_oauth2::{service_account_key_from_file, ServiceAccountAccess};

/// Type alias for authenticating against GCP based on service accounts
pub type GcpServiceAccess = ServiceAccountAccess<hyper::client::Client>;

/// Type alias for access to Google Cloud storage
pub type GcsHub = Storage<hyper::client::Client, GcpServiceAccess>;

/// Type alias for access to Google PubSub
pub type GpsHub = pubsub::Pubsub<hyper::client::Client, GcpServiceAccess>;

/// Authenticate against GCP using a service account json secrets file
///  and returns a Google Cloud Storage API binding
pub fn authenticate(secrets_file: &str) -> Result<GcpServiceAccess> {
    // Load and parse the service account secrets file
    let key = service_account_key_from_file(&secrets_file.to_string())?;

    // Create a HTTPS client
    let client = hyper::Client::with_connector(HttpsConnector::new(hyper_rustls::TlsClient::new()));

    // Authenticate against GCP
    let access = ServiceAccountAccess::new(key, client);

    Ok(access)
}

pub fn storage_api(access: GcpServiceAccess) -> Result<GcsHub> {
    // Bind against GCS
    Ok(Storage::new(
        hyper::Client::with_connector(hyper::net::HttpsConnector::new(
            hyper_rustls::TlsClient::new(),
        )),
        access,
    ))
}

pub fn pubsub_api(access: GcpServiceAccess) -> Result<GpsHub> {
    // Bind against GPS
    Ok(pubsub::Pubsub::new(
        hyper::Client::with_connector(hyper::net::HttpsConnector::new(
            hyper_rustls::TlsClient::new(),
        )),
        access,
    ))
}

/// When logging level is error or debug, print the result of each
/// request to stdout
pub fn verbose(
    result: google_storage1::Result<(hyper::client::response::Response, google_storage1::Object)>,
) -> google_storage1::Result<(hyper::client::response::Response, google_storage1::Object)> {
    if log_enabled!(Level::Error) || log_enabled!(Level::Debug) {
        match &result {
            Err(e) => match e {
                Error::HttpError(_)
                | Error::MissingAPIKey
                | Error::MissingToken(_)
                | Error::Cancelled
                | Error::UploadSizeLimitExceeded(_, _)
                | Error::Failure(_)
                | Error::BadRequest(_)
                | Error::FieldClash(_)
                | Error::JsonDecodeError(_, _) => println!("Error: {}", e),
            },
            Ok(res) => println!("Success: {:?}", res),
        };
    }
    result
}
