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

use crate::errors::Result;
/// Using `GOOGLE_APPLICATION_CREDENTIALS="<path to service token json file>"` this function
/// will authenticate against the google cloud platform using the authentication flow defined
/// in the file. The provided PEM file should be a current non-revoked and complete copy of the
/// required certificate chain for the Google Cloud Platform.
use gouth::Token;
use reqwest::header::HeaderMap;
use reqwest::Client;

#[cfg(not(tarpaulin_include))]
pub(crate) struct GcsClient {
    client: Client,
    token: gouth::Token,
}

#[cfg(not(tarpaulin_include))]
impl GcsClient {
    // Create a wrapper around reqwest::client
    // which keeps the token with itself and inserts on each request.

    pub fn get(&self, url: String) -> Result<reqwest::RequestBuilder> {
        let token: String = self.token.header_value()?.to_string();

        Ok(self.client.get(url).header(
            "authorization",
            reqwest::header::HeaderValue::from_maybe_shared(token)?,
        ))
    }

    pub fn post(&self, url: String) -> Result<reqwest::RequestBuilder> {
        let token: String = self.token.header_value()?.to_string();

        Ok(self.client.post(url).header(
            "authorization",
            reqwest::header::HeaderValue::from_maybe_shared(token)?,
        ))
    }

    pub fn delete(&self, url: String) -> Result<reqwest::RequestBuilder> {
        let token: String = self.token.header_value()?.to_string();

        Ok(self.client.delete(url).header(
            "authorization",
            reqwest::header::HeaderValue::from_maybe_shared(token)?,
        ))
    }
}

/// Returns the gcs json-api-client
pub(crate) fn json_api_client(extra_headers: &HeaderMap) -> Result<GcsClient> {
    // create a new goauth::Token.
    let token = Token::new()?;
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "content-type",
        reqwest::header::HeaderValue::from_static("application/json"),
    );
    for header in extra_headers {
        headers.append(header.0, header.1.clone());
    }
    Ok(GcsClient {
        token,
        client: reqwest::Client::builder()
            .default_headers(headers)
            .build()?,
    })
}
