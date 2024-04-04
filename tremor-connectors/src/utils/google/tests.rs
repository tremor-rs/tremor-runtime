// Copyright 2024, The Tremor Team
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

use crate::utils::integration::free_port;

use super::*;
use hyper::{
    service::{make_service_fn, service_fn},
    Body,
};
use std::{convert::Infallible, io::Write, net::ToSocketAddrs};

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
impl From<TokenSrc> for TestTokenProvider {
    fn from(_src: TokenSrc) -> Self {
        Self::new()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key_id: String,
    private_key: String,
    token_uri: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TokenResponse {
    token_type: String,
    access_token: String,
    expires_in: u64,
}

/// Some random generated private key that isn't used anywhere else
const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC/SZoFm3528gDJ
vMQBeTGm6dohSfqstFoYYVtGEDGnt9GwkjbJcnIAIiON+Qw7wV5v24UFJKQ8Eg/q
Jf8bF0PT6yvSW+cof/94OgGz/PyPwrHVGniEy2Wbe1qYkDaQfxDzyPP5hKetmoof
FF8u1IyJYdduxBm80eYG/JVYhn85ycV4zVUWPzuF7BmBmK4n1DX8HlD3qQWtVtiP
DCQ1H7pKSn6nDLlQtv6zEx5gnfnVIC/G2hB414FqTxkwLI5ae5njOeh9aFzTzD5Y
hifcPqjs91fJ4tO4/VfesyrOWOowAIil7ZaWNd6CsljiC0iqt15oohBKbFz/wGSv
DxTiavvRAgMBAAECggEAAT9Rd/IxLPhItu5z7ovthE7eK2oZ1OjFKKEKSq0eDpLe
7p8sqJVTA65O6ItXjNRm0WU1tOU6nyJBnjXnhLP0lYWhG5Lm8W23Cv/n1TzHIdUN
bbWpoQYMttEv87KgpHV4dRQaB5LzOMLUxHCdauCbo2UZSRSrk7HG5ZDdx9eMR1Wg
vkhk3S70dyheO804BwSkvpxCbjcgg2ILRn5EacL0uU7GNxGQUCInNK2LTN0gUSKg
qLITAE2CE0cwcs6DzPgHk3M78AlTILDYbKmOIB3FPImTY88crR9OMvqDbraKTvwb
sS2M5gWOO0LDOeXVuIxG9j0J3hxxSY6aGHJRt+d5BQKBgQDLQ3Ri6OXirtd2gxZv
FY65lHQd+LMrWO2R31zv2aif+XoJRh5PXM5kN5Cz6eFp/z2E5DWa1ubF4nPSBc3S
fW96LGwnBLOIOccxJ6wdfLY+sw/U2PEDhUP5Z0NxHr4x0AOxfQTrEmnSyx6oE04Q
rXtqpiCg8pP+za6Hx1ZWFx1YxQKBgQDw6rbv+Wadz+bnuOYWyy7GUv7ZXVWup1gU
IoZgR5h6ZMNyFpK2NlzLOctzttkWdoV9fn4ux6T3kBWrJdbd9WkCGom2SX6b3PqH
evcZ73RvbuHVjtm9nHov9eqU+pcz8Se3NZVEhsov1FWboBE5E+i1qO0jiOaJRFEm
aIlaK9gPnQKBgDkmx0PETlb1aDm3VAh53D6L4jZHJkGK6Il6b0w1O/d3EvwmjgEs
jA+bnAEqQqomDSsfa38U66A6MuybmyqTAFQux14VMVGdRUep6vgDh86LVGk5clLW
Fq26fjkBNuMUpOUzzL032S9e00jY3LtNvATZnxUB/+DF/kvJHZppN2QtAoGAB/7S
KW6ugChJMoGJaVI+8CgK+y3EzTISk0B+Ey3tGorDjcLABboSJFB7txBnbf5q+bo7
99N6XxjyDycHVYByhrZYwar4v7V6vwpOrxaqV5RnfE3sXgWWbIcNzPnwELI9LjBi
Ds8mYKX8XVjXmXxWqci8bgR6Gi4hP1QS0uJHnmUCgYEAiDbOiUed1gL1yogrTG4w
r+S/aL2pt/xBNz9Dw+cZnqnZHWDuewU8UCO6mrupM8MXEAfRnzxyUX8b7Yk/AoFo
sEUlZGvHmBh8nBk/7LJVlVcVRWQeQ1kg6b+m6thwRz6HsKIvExpNYbVkzqxbeJW3
PX8efvDMhv16QqDFF0k80d0=
-----END PRIVATE KEY-----";

#[tokio::test(flavor = "multi_thread")]
async fn gouth_token() -> anyhow::Result<()> {
    let mut file = tempfile::NamedTempFile::new()?;

    let port = free_port::find_free_tcp_port().await?;
    let sa = ServiceAccount {
        client_email: "snot@tremor.rs".to_string(),
        private_key_id: "badger".to_string(),
        private_key: PRIVATE_KEY.to_string(),
        token_uri: format!("http://127.0.0.1:{port}/"),
    };
    let sa_str = simd_json::serde::to_string_pretty(&sa)?;
    file.as_file_mut().write_all(sa_str.as_bytes())?;
    let path = file.into_temp_path();
    let path_str = path.to_string_lossy().to_string();

    let mut provider = GouthTokenProvider::from(TokenSrc::File(path_str));
    dbg!(&provider);
    assert!(provider.get_token().is_err());

    let service_fn = make_service_fn(|_| async {
        Ok::<_, Infallible>(service_fn(|_| async {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(hyper::Response::builder().body(
                Body::from(simd_json::serde::to_vec(&TokenResponse {
                    token_type: "snot".to_string(),
                    access_token: "access_token".to_string(),
                    expires_in: 100_000_000,
                })?),
            )?)
        }))
    });

    let addr = ("127.0.0.1", port)
        .to_socket_addrs()?
        .next()
        .expect("no address");
    let server_handle = tokio::task::spawn(async move {
        let listener = hyper::Server::bind(&addr).serve(service_fn);
        listener.await
    });
    // Make sure the server is up by retrying a few times
    let mut attempt = 0;
    let token = loop {
        if let Ok(token) = provider.get_token() {
            break token;
        }
        assert!(attempt < 20, "Failed to get token");
        attempt += 1;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert_eq!(token.as_str(), "snot access_token");

    server_handle.abort();

    // token is cached, no need to call again
    let token = provider.get_token()?;
    assert_eq!(token.as_str(), "snot access_token");

    Ok(())
}

#[test]
fn appease_the_coverage_gods() {
    let provider = GouthTokenProvider::from(TokenSrc::dummy());
    let mut provider = provider;
    assert!(provider.get_token().is_err());

    let provider = FailingTokenProvider::default();
    let mut provider = provider;
    assert!(provider.get_token().is_err());
}

#[test]
fn interceptor_can_add_the_auth_header() -> anyhow::Result<()> {
    let mut interceptor = AuthInterceptor {
        token_provider: TestTokenProvider::new_with_token(Arc::new("test".to_string())),
    };
    let request = Request::new(());

    let result = interceptor.call(request)?;

    assert!(result
        .metadata()
        .get("authorization")
        .map(|m| m == "test")
        .unwrap_or_default());
    Ok(())
}

#[derive(Clone, Default)]
struct FailingTokenProvider {}

impl TokenProvider for FailingTokenProvider {
    fn get_token(&mut self) -> std::result::Result<Arc<String>, Status> {
        Err(Status::unavailable("boo"))
    }
}
impl From<TokenSrc> for FailingTokenProvider {
    fn from(_src: TokenSrc) -> Self {
        Self::default()
    }
}

#[test]
fn interceptor_will_pass_token_error() {
    let mut interceptor = AuthInterceptor {
        token_provider: FailingTokenProvider {},
    };
    let request = Request::new(());

    let result = interceptor.call(request);

    assert!(result.is_err());
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
