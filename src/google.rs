// Copyright 2018-2020, Wayfair GmbH
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

use hyper::client::Client;
use log::Level;
use std::io::Result;
use yup_oauth2::{service_account_key_from_file, ServiceAccountAccess};

/// Type alias for authenticating against GCP based on service accounts

/// Type alias for access to Google Cloud storage
pub(crate) type GcsHub = google_storage1::Storage<Client, ServiceAccountAccess<Client>>;

/// Type alias for access to Google `PubSub`
pub(crate) type GpsHub = google_pubsub1::Pubsub<Client, ServiceAccountAccess<Client>>;

pub(crate) fn storage_api(secrets_file: &str) -> Result<GcsHub> {
    let client = hyper::Client::with_connector(hyper::net::HttpsConnector::new(
        hyper_rustls::TlsClient::new(),
    ));
    let secret = service_account_key_from_file(&secrets_file.to_string())?;
    let access = ServiceAccountAccess::new(secret, client);
    let client = hyper::Client::with_connector(hyper::net::HttpsConnector::new(
        hyper_rustls::TlsClient::new(),
    ));
    Ok(GcsHub::new(client, access))
}

pub(crate) fn pubsub_api(secrets_file: &str) -> Result<GpsHub> {
    let client = hyper::Client::with_connector(hyper::net::HttpsConnector::new(
        hyper_rustls::TlsClient::new(),
    ));
    let secret = service_account_key_from_file(&secrets_file.to_string())?;
    let access = ServiceAccountAccess::new(secret, client);
    let client = hyper::Client::with_connector(hyper::net::HttpsConnector::new(
        hyper_rustls::TlsClient::new(),
    ));
    Ok(GpsHub::new(client, access))
}

/// When logging level is error or debug, print the result of each
/// request to stdout
pub(crate) fn verbose(
    result: google_storage1::Result<(hyper::client::Response, google_storage1::Object)>,
) -> google_storage1::Result<(hyper::client::Response, google_storage1::Object)> {
    if log_enabled!(Level::Error) || log_enabled!(Level::Debug) {
        match &result {
            Err(e) => match e {
                google_storage1::Error::HttpError(_)
                | google_storage1::Error::MissingAPIKey
                | google_storage1::Error::MissingToken(_)
                | google_storage1::Error::Cancelled
                | google_storage1::Error::UploadSizeLimitExceeded(_, _)
                | google_storage1::Error::Failure(_)
                | google_storage1::Error::BadRequest(_)
                | google_storage1::Error::FieldClash(_)
                | google_storage1::Error::JsonDecodeError(_, _) => println!("Error: {}", e),
            },
            Ok(res) => println!("Success: {:?}", res),
        };
    }
    result
}
