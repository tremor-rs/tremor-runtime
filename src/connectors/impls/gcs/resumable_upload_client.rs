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

use crate::connectors::impls::gcs::chunked_buffer::BufferPart;
use crate::connectors::prelude::{Result, Url};
use crate::connectors::utils::url::HttpsDefaults;
use crate::errors::err_gcs;
use async_std::task::sleep;
#[cfg(not(test))]
use gouth::Token;
use http_client::HttpClient;
use http_types::{Body, Method, Request, Response, StatusCode};
use std::time::Duration;

#[async_trait::async_trait]
pub(crate) trait ResumableUploadClient {
    async fn start_upload(&mut self, url: &Url<HttpsDefaults>, file_id: FileId)
        -> Result<url::Url>;
    async fn upload_data(&mut self, url: &url::Url, part: BufferPart) -> Result<usize>;
    async fn finish_upload(&mut self, url: &url::Url, part: BufferPart) -> Result<()>;
    async fn delete_upload(&mut self, url: &url::Url) -> Result<()>;
    async fn bucket_exists(&mut self, url: &Url<HttpsDefaults>, bucket: &str) -> Result<bool>;
}

pub(crate) trait BackoffStrategy {
    fn wait_time(&self, retry_index: u32) -> Duration;
    fn max_retries(&self) -> u32;
}

pub(crate) struct ExponentialBackoffRetryStrategy {
    max_retries: u32,
    base_sleep_time: Duration,
}

impl ExponentialBackoffRetryStrategy {
    pub fn new(max_retries: u32, base_sleep_time: Duration) -> Self {
        Self {
            max_retries,
            base_sleep_time,
        }
    }
}

impl BackoffStrategy for ExponentialBackoffRetryStrategy {
    fn wait_time(&self, retry_index: u32) -> Duration {
        self.base_sleep_time * 2u32.pow(retry_index)
    }

    fn max_retries(&self) -> u32 {
        self.max_retries
    }
}

async fn retriable_request<
    TBackoffStrategy: BackoffStrategy,
    THttpClient: HttpClient,
    TMakeRequest: Fn() -> Result<Request>,
>(
    backoff_strategy: &TBackoffStrategy,
    client: &mut THttpClient,
    make_request: TMakeRequest,
) -> Result<Response> {
    let max_retries = backoff_strategy.max_retries();
    for i in 1..=max_retries {
        let request = make_request();
        let error_wait_time = backoff_strategy.wait_time(i);

        match request {
            Ok(request) => {
                let result = client.send(request).await;

                match result {
                    Ok(mut response) => {
                        if response.status().is_server_error() {
                            let response_body = response
                                .body_string()
                                .await
                                .unwrap_or_else(|_| "<unable to read body>".to_string());

                            warn!(
                                "Request {}/{} failed - Server error: {}",
                                i, max_retries, response_body
                            );
                            sleep(error_wait_time).await;
                            continue;
                        }

                        return Ok(response);
                    }
                    Err(error) => {
                        warn!("Request {}/{} failed: {}", i, max_retries, error);
                        sleep(error_wait_time).await;
                        continue;
                    }
                }
            }
            Err(error) => {
                warn!(
                    "Request {}/{} failed to be created: {}",
                    i, max_retries, error
                );
                sleep(error_wait_time).await;
                continue;
            }
        }
    }

    Err(err_gcs(format!(
        "Request still failing after {max_retries} retries"
    )))
}

pub(crate) struct DefaultClient<THttpClient: HttpClient, TBackoffStrategy: BackoffStrategy> {
    #[cfg(not(test))]
    token: Token,
    client: THttpClient,
    backoff_strategy: TBackoffStrategy,
}

#[async_trait::async_trait]
impl<THttpClient: HttpClient, TBackoffStrategy: BackoffStrategy + Send + Sync> ResumableUploadClient
    for DefaultClient<THttpClient, TBackoffStrategy>
{
    async fn bucket_exists(&mut self, url: &Url<HttpsDefaults>, bucket: &str) -> Result<bool> {
        let mut response = retriable_request(&self.backoff_strategy, &mut self.client, || {
            let url = url::Url::parse(&format!("{}/b/{}", url, bucket))?;
            Ok(Request::new(Method::Get, url))
        })
        .await?;
        let status = response.status();
        if status.is_server_error() {}
        if status == StatusCode::NotFound {
            Ok(false)
        } else if status.is_success() {
            debug!(
                "Bucket {bucket} metadata: {}",
                response.body_string().await?
            );
            Ok(true)
        } else {
            // assuming error
            error!(
                "Error checking that bucket exists: {}",
                response.body_string().await?
            );
            return Err(err_gcs(format!(
                "Check if bucket {} exists failed with {} status",
                bucket,
                response.status()
            )));
        }
    }

    async fn start_upload(
        &mut self,
        url: &Url<HttpsDefaults>,
        file_id: FileId,
    ) -> Result<url::Url> {
        let mut response = retriable_request(&self.backoff_strategy, &mut self.client, || {
            Self::create_upload_start_request(
                #[cfg(not(test))]
                &self.token,
                url,
                &file_id,
            )
        })
        .await?;

        if response.status().is_server_error() {
            error!(
                "Error from Google Cloud Storage: {}",
                response.body_string().await?
            );

            return Err(err_gcs(format!(
                "Start upload failed with {} status",
                response.status()
            )));
        }

        Ok(url::Url::parse(
            response
                .header("Location")
                .ok_or_else(|| err_gcs("Missing Location header".to_string()))?
                .last()
                .as_str(),
        )?)
    }

    async fn upload_data(&mut self, url: &url::Url, part: BufferPart) -> Result<usize> {
        let mut response = retriable_request(&self.backoff_strategy, &mut self.client, || {
            let mut request = Request::new(Method::Put, url.clone());

            request.insert_header(
                "Content-Range",
                format!(
                    "bytes {}-{}/*",
                    part.start,
                    // -1 on the end is here, because Content-Range is inclusive and our range is exclusive
                    part.start + part.len() - 1
                ),
            );
            request.insert_header("User-Agent", "Tremor");
            request.insert_header("Accept", "*/*");
            request.set_body(part.data.clone());

            Ok(request)
        })
        .await?;

        if response.status().is_server_error() {
            error!(
                "Error from Google Cloud Storage: {}",
                response.body_string().await?
            );
            return Err(err_gcs(format!(
                "Start upload failed with status {}",
                response.status()
            )));
        }

        let raw_range = response
            .header("range")
            .ok_or_else(|| err_gcs("No range header"))?;
        let raw_range = raw_range
            .get(0)
            .ok_or_else(|| err_gcs("Missing Range header value"))?
            .as_str();

        // Range format: bytes=0-262143
        let range_end = &raw_range
            .get(
                raw_range
                    .find('-')
                    .ok_or_else(|| err_gcs("Did not find a - in the Range header"))?
                    + 1..,
            )
            .ok_or_else(|| err_gcs("Unable to get the end of the Range"))?;
        Ok(range_end.parse()?)
    }

    async fn delete_upload(&mut self, url: &url::Url) -> Result<()> {
        let mut response = retriable_request(&self.backoff_strategy, &mut self.client, || {
            let mut request = Request::new(Method::Delete, url.clone());
            request.set_body(Body::empty()); // ensure content-range: 0 header
            Ok(request)
        })
        .await?;
        // dont ask me, see: https://cloud.google.com/storage/docs/performing-resumable-uploads#cancel-upload
        if response.status() as u32 == 499 {
            Ok(())
        } else {
            error!(
                "Error from Google Cloud Storage while cancelling an upload: {}",
                response.body_string().await?
            );
            Err(err_gcs(format!(
                "Delete upload failed with status {}",
                response.status()
            )))
        }
    }

    async fn finish_upload(&mut self, url: &url::Url, part: BufferPart) -> Result<()> {
        let mut response = retriable_request(&self.backoff_strategy, &mut self.client, || {
            let mut request = Request::new(Method::Put, url.clone());

            request.insert_header(
                "Content-Range",
                format!(
                    "bytes {}-{}/{}",
                    part.start,
                    // -1 on the end is here, because Content-Range is inclusive
                    part.start + part.len() - 1,
                    part.start + part.len()
                ),
            );

            request.set_body(part.data.clone());

            Ok(request)
        })
        .await?;

        if response.status().is_server_error() {
            error!(
                "Error from Google Cloud Storage: {}",
                response.body_string().await?
            );

            return Err(err_gcs(format!(
                "Finish upload failed with status {}",
                response.status()
            )));
        }

        Ok(())
    }
}
impl<THttpClient: HttpClient, TBackoffStrategy: BackoffStrategy>
    DefaultClient<THttpClient, TBackoffStrategy>
{
    pub fn new(client: THttpClient, backoff_strategy: TBackoffStrategy) -> Result<Self> {
        Ok(Self {
            #[cfg(not(test))]
            token: Token::new()?,
            client,
            backoff_strategy,
        })
    }

    #[cfg(test)]
    fn create_upload_start_request(url: &Url<HttpsDefaults>, file: &FileId) -> Result<Request> {
        let url = url::Url::parse(&format!(
            "{}/b/{}/o?name={}&uploadType=resumable",
            url, file.bucket, file.name
        ))?;
        let request = Request::new(Method::Post, url);

        Ok(request)
    }

    #[cfg(not(test))]
    fn create_upload_start_request(
        token: &Token,
        url: &Url<HttpsDefaults>,
        file: &FileId,
    ) -> Result<Request> {
        let url = url::Url::parse(&format!(
            "{}/b/{}/o?name={}&uploadType=resumable",
            url, file.bucket, file.name
        ))?;
        let mut request = Request::new(Method::Post, url);
        request.insert_header("Authorization", token.header_value()?.to_string());

        Ok(request)
    }
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub(crate) struct FileId {
    pub bucket: String,
    pub name: String,
}

impl FileId {
    pub(super) fn new(bucket: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            bucket: bucket.into(),
        }
    }
}

impl std::fmt::Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "gs://{}/{}", self.bucket, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task::block_on;
    use http_types::{Error, Response, StatusCode};
    use std::fmt::{Debug, Formatter};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    pub(crate) struct MockHttpClient {
        pub config: http_client::Config,
        pub handle_request:
            Box<dyn Fn(http_client::Request) -> std::result::Result<Response, Error> + Send + Sync>,
        pub simulate_failure: Arc<AtomicBool>,
        pub simulate_transport_failure: Arc<AtomicBool>,
    }

    impl Debug for MockHttpClient {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "<mock>")
        }
    }

    #[async_trait::async_trait]
    impl HttpClient for MockHttpClient {
        async fn send(&self, req: http_client::Request) -> std::result::Result<Response, Error> {
            if self
                .simulate_transport_failure
                .swap(false, Ordering::AcqRel)
            {
                return Err(Error::new(
                    StatusCode::InternalServerError,
                    anyhow::Error::msg("injected error"),
                ));
            }

            if self.simulate_failure.swap(false, Ordering::AcqRel) {
                return Ok(Response::new(StatusCode::InternalServerError));
            }

            (self.handle_request)(req)
        }

        fn set_config(&mut self, config: http_client::Config) -> http_types::Result<()> {
            self.config = config;

            Ok(())
        }

        fn config(&self) -> &http_client::Config {
            &self.config
        }
    }

    #[async_std::test]
    pub async fn can_start_upload() -> Result<()> {
        let client = MockHttpClient {
            config: Default::default(),
            handle_request: Box::new(|req| {
                assert_eq!(req.url().path(), "/upload/b/bucket/o");
                assert_eq!(
                    req.url().query().unwrap(),
                    "name=somefile&uploadType=resumable".to_string()
                );

                let mut response = Response::new(StatusCode::PermanentRedirect);
                response.insert_header("Location", "http://example.com/upload_session");
                Ok(response)
            }),
            simulate_failure: Arc::new(AtomicBool::new(true)),
            simulate_transport_failure: Arc::new(AtomicBool::new(true)),
        };
        let mut api_client = DefaultClient {
            client,
            backoff_strategy: ExponentialBackoffRetryStrategy {
                max_retries: 3,
                base_sleep_time: Duration::from_nanos(1),
            },
        };
        api_client
            .start_upload(
                &Url::parse("http://example.com/upload").expect("static url did not parse"),
                FileId::new("bucket", "somefile"),
            )
            .await?;
        Ok(())
    }

    #[async_std::test]
    pub async fn can_upload_data() -> Result<()> {
        let client = MockHttpClient {
            config: Default::default(),
            handle_request: Box::new(|mut req| {
                let body = block_on(req.body_bytes()).unwrap();
                assert_eq!(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], body);
                assert_eq!(req.header("Content-Range").unwrap()[0], "bytes 0-9/*");

                let mut response = Response::new(StatusCode::Ok);
                response.insert_header("Range", "bytes=0-10");
                Ok(response)
            }),
            simulate_failure: Arc::new(AtomicBool::new(true)),
            simulate_transport_failure: Arc::new(AtomicBool::new(true)),
        };
        let session_url = url::Url::parse("https://example.com/session").unwrap();
        let mut api_client = DefaultClient {
            client,
            backoff_strategy: ExponentialBackoffRetryStrategy {
                max_retries: 3,
                base_sleep_time: Duration::from_nanos(1),
            },
        };
        let data = BufferPart {
            data: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            start: 0,
        };

        let res = api_client.upload_data(&session_url, data).await?;
        assert_eq!(10, res);
        Ok(())
    }

    #[async_std::test]
    pub async fn upload_data_fails_on_failed_request() -> Result<()> {
        let client = MockHttpClient {
            config: Default::default(),
            handle_request: Box::new(|_req| Ok(Response::new(StatusCode::InternalServerError))),
            simulate_failure: Arc::new(AtomicBool::new(true)),
            simulate_transport_failure: Arc::new(AtomicBool::new(true)),
        };
        let session_url = url::Url::parse("https://example.com/session").unwrap();
        let mut api_client = DefaultClient {
            client,
            backoff_strategy: ExponentialBackoffRetryStrategy {
                max_retries: 3,
                base_sleep_time: Duration::from_nanos(1),
            },
        };
        let data = BufferPart {
            data: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            start: 0,
        };
        let result = api_client.upload_data(&session_url, data).await;

        assert!(result.is_err());
        Ok(())
    }

    #[async_std::test]
    pub async fn can_finish_upload() -> Result<()> {
        let client = MockHttpClient {
            config: Default::default(),
            handle_request: Box::new(|req| {
                assert_eq!(req.header("Content-Range").unwrap()[0], "bytes 10-12/13");

                Ok(Response::new(StatusCode::Ok))
            }),
            simulate_failure: Arc::new(AtomicBool::new(true)),
            simulate_transport_failure: Arc::new(AtomicBool::new(true)),
        };

        let session_url = url::Url::parse("https://example.com/session").unwrap();
        let mut api_client = DefaultClient {
            client,
            backoff_strategy: ExponentialBackoffRetryStrategy {
                max_retries: 3,
                base_sleep_time: Duration::from_nanos(1),
            },
        };
        let data = BufferPart {
            data: vec![1, 2, 3],
            start: 10,
        };
        api_client.finish_upload(&session_url, data).await?;
        Ok(())
    }

    #[async_std::test]
    async fn retries_on_server_error() {
        let request_handled = Arc::new(AtomicBool::new(false));
        let request_handled_clone = request_handled.clone();

        let response = retriable_request(
            &ExponentialBackoffRetryStrategy::new(3, Duration::from_nanos(1)),
            &mut MockHttpClient {
                config: Default::default(),
                handle_request: Box::new(move |_req| {
                    request_handled_clone.swap(true, Ordering::Acquire);

                    Ok(Response::new(StatusCode::Ok))
                }),
                simulate_failure: Arc::new(AtomicBool::new(true)),
                simulate_transport_failure: Arc::new(Default::default()),
            },
            || Ok(Request::new(Method::Get, "http://example.com")),
        )
        .await
        .unwrap();

        assert!(request_handled.load(Ordering::Acquire));
        assert_eq!(response.status(), StatusCode::Ok);
    }

    #[async_std::test]
    async fn fails_when_retries_are_exhausted() {
        let response = retriable_request(
            &ExponentialBackoffRetryStrategy::new(3, Duration::from_nanos(1)),
            &mut MockHttpClient {
                config: Default::default(),
                handle_request: Box::new(move |_req| {
                    Ok(Response::new(StatusCode::InternalServerError))
                }),
                simulate_failure: Arc::new(AtomicBool::new(true)),
                simulate_transport_failure: Arc::new(Default::default()),
            },
            || Ok(Request::new(Method::Get, "http://example.com")),
        )
        .await;

        assert!(response.is_err());
    }

    #[async_std::test]
    async fn retries_on_request_creation_failure() {
        let request_handled = Arc::new(AtomicBool::new(false));
        let request_handled_clone = request_handled.clone();

        let response = retriable_request(
            &ExponentialBackoffRetryStrategy::new(3, Duration::from_nanos(1)),
            &mut MockHttpClient {
                config: Default::default(),
                handle_request: Box::new(move |_req| Ok(Response::new(StatusCode::Ok))),
                simulate_failure: Arc::new(AtomicBool::new(true)),
                simulate_transport_failure: Arc::new(Default::default()),
            },
            || {
                if !request_handled_clone.swap(true, Ordering::Acquire) {
                    return Err("boo".into());
                }

                Ok(Request::new(Method::Get, "http://example.com"))
            },
        )
        .await
        .unwrap();

        assert!(request_handled.load(Ordering::Acquire));
        assert_eq!(response.status(), StatusCode::Ok);
    }

    #[test]
    pub fn mock_http_client_config() {
        let mut client = MockHttpClient {
            config: Default::default(),
            handle_request: Box::new(|_| Ok(Response::new(StatusCode::Ok))),
            simulate_failure: Arc::new(Default::default()),
            simulate_transport_failure: Arc::new(Default::default()),
        };

        let mut config = http_client::Config::new();
        config.timeout = Some(Duration::from_secs(1000));

        client.set_config(config.clone()).unwrap();

        assert_eq!(client.config().timeout, config.timeout);
    }
}
