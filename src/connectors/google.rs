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

use gouth::Token;
use std::sync::Arc;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};

pub trait TokenProvider: Clone + Default + Send {
    fn get_token(&mut self) -> ::std::result::Result<Arc<String>, Status>;
}

pub struct GouthTokenProvider {
    gouth_token: Option<Token>,
}

impl Clone for GouthTokenProvider {
    fn clone(&self) -> Self {
        Self { gouth_token: None }
    }
}

impl Default for GouthTokenProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl GouthTokenProvider {
    pub fn new() -> Self {
        GouthTokenProvider { gouth_token: None }
    }
}

impl TokenProvider for GouthTokenProvider {
    fn get_token(&mut self) -> ::std::result::Result<Arc<String>, Status> {
        let token = if let Some(ref token) = self.gouth_token {
            token
        } else {
            let new_token =
                Token::new().map_err(|_| Status::unavailable("Failed to read Google Token"))?;

            self.gouth_token.get_or_insert(new_token)
        };

        token
            .header_value()
            .map_err(|_e| Status::unavailable("Failed to read the Google Token header value"))
    }
}

#[derive(Clone)]
pub(crate) struct AuthInterceptor<T>
where
    T: TokenProvider,
{
    pub token_provider: T,
}

impl<T> Interceptor for AuthInterceptor<T>
where
    T: TokenProvider,
{
    fn call(&mut self, mut request: Request<()>) -> ::std::result::Result<Request<()>, Status> {
        let header_value = self.token_provider.get_token()?;
        let metadata_value = match MetadataValue::from_str(header_value.as_str()) {
            Ok(val) => val,
            Err(e) => {
                error!("Failed to get token for BigQuery: {}", e);

                return Err(Status::unavailable(
                    "Failed to retrieve authentication token.",
                ));
            }
        };
        request
            .metadata_mut()
            .insert("authorization", metadata_value);

        Ok(request)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[derive(Clone)]
    pub struct TestTokenProvider {
        token: Arc<String>,
    }

    impl Default for TestTokenProvider {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TestTokenProvider {
        pub fn new() -> Self {
            Self {
                token: Arc::new(String::new()),
            }
        }

        pub fn new_with_token(token: Arc<String>) -> Self {
            Self { token }
        }
    }

    impl TokenProvider for TestTokenProvider {
        fn get_token(&mut self) -> ::std::result::Result<Arc<String>, Status> {
            Ok(self.token.clone())
        }
    }

    #[test]
    fn interceptor_can_add_the_auth_header() {
        let mut interceptor = AuthInterceptor {
            token_provider: TestTokenProvider::new_with_token(Arc::new("test".to_string())),
        };
        let request = Request::new(());

        let result = interceptor.call(request).unwrap();

        assert_eq!(result.metadata().get("authorization").unwrap(), "test");
    }

    #[derive(Clone)]
    struct FailingTokenProvider {}

    impl Default for FailingTokenProvider {
        fn default() -> Self {
            Self {}
        }
    }

    impl TokenProvider for FailingTokenProvider {
        fn get_token(&mut self) -> Result<Arc<String>, Status> {
            Err(Status::unavailable("boo"))
        }
    }

    #[test]
    fn interceptor_will_pass_token_error() {
        let mut interceptor = AuthInterceptor {
            token_provider: FailingTokenProvider {},
        };
        let request = Request::new(());

        let result = interceptor.call(request);

        assert_eq!(result.unwrap_err().message(), "boo");
    }

    #[test]
    fn interceptor_fails_on_invalid_token_value() {
        let mut interceptor = AuthInterceptor {
            // control characters (ASCII < 32) are not allowed
            token_provider: TestTokenProvider::new_with_token(Arc::new("\r\n".into())),
        };
        let request = Request::new(());

        let result = interceptor.call(request);

        assert!(result.is_err());
    }
}
