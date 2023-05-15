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

use crate::{
    connectors::{
        google::TokenSrc,
        prelude::{Result, Url},
        utils::{
            object_storage::{BufferPart, ObjectId},
            url::HttpsDefaults,
        },
    },
    errors::err_gcs,
};
#[cfg(not(test))]
use gouth::Token;
use http_body::Body as BodyTrait;
use hyper::{header, Body, Method, Request, Response, StatusCode};
use hyper_rustls::HttpsConnectorBuilder;
use std::time::Duration;
use tokio::time::sleep;

pub(crate) type GcsHttpClient =
    hyper::Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>;

#[async_trait::async_trait]
pub(crate) trait ResumableUploadClient {
    async fn start_upload(
        &mut self,
        url: &Url<HttpsDefaults>,
        file_id: ObjectId,
    ) -> Result<url::Url>;
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
    TClient: HttpClientTrait,
    TBackoffStrategy: BackoffStrategy,
    TMakeRequest: Fn() -> Result<hyper::Request<hyper::Body>>,
>(
    backoff_strategy: &TBackoffStrategy,
    client: &mut TClient,
    make_request: TMakeRequest,
) -> Result<Response<Body>> {
    let max_retries = backoff_strategy.max_retries();
    for i in 1..=max_retries {
        let request = make_request();
        let error_wait_time = backoff_strategy.wait_time(i);

        match request {
            Ok(request) => {
                let result = client.request(request).await;

                match result {
                    Ok(mut response) => {
                        if response.status().is_server_error() {
                            let mut response_body: Vec<u8> = Vec::new();
                            while let Some(chunk) = response.data().await.transpose()? {
                                response_body.extend_from_slice(&chunk);
                            }
                            let response_body = String::from_utf8(response_body)?;

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

pub(crate) struct DefaultClient<TClient: HttpClientTrait, TBackoffStrategy: BackoffStrategy> {
    #[cfg(not(test))]
    token: Token,
    client: TClient,
    backoff_strategy: TBackoffStrategy,
}

#[async_trait::async_trait]
impl<TClient: HttpClientTrait, TBackoffStrategy: BackoffStrategy + Send + Sync>
    ResumableUploadClient for DefaultClient<TClient, TBackoffStrategy>
{
    async fn bucket_exists(&mut self, url: &Url<HttpsDefaults>, bucket: &str) -> Result<bool> {
        let mut response = retriable_request(&self.backoff_strategy, &mut self.client, || {
            let url = format!("{url}b/{bucket}");
            Ok(Request::builder()
                .method(Method::GET)
                .uri(url)
                .body(Body::empty())?)
        })
        .await?;
        let status = response.status();
        if status.is_server_error() {}
        let mut data: Vec<u8> = Vec::new();
        while let Some(chunk) = response.data().await.transpose()? {
            data.extend_from_slice(&chunk);
        }
        if status == StatusCode::NOT_FOUND {
            Ok(false)
        } else if status.is_success() {
            let body_string = String::from_utf8(data)?;
            debug!("Bucket {bucket} metadata: {body_string}",);
            Ok(true)
        } else {
            let body_string = String::from_utf8(data)?;
            // assuming error
            error!("Error checking that bucket exists: {body_string}",);
            return Err(err_gcs(format!(
                "Check if bucket {bucket} exists failed with {} status",
                response.status()
            )));
        }
    }

    async fn start_upload(
        &mut self,
        url: &Url<HttpsDefaults>,
        file_id: ObjectId,
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
            let mut data: Vec<u8> = Vec::new();
            while let Some(chunk) = response.data().await.transpose()? {
                data.extend_from_slice(&chunk);
            }
            let body_string = String::from_utf8(data)?;
            error!("Error from Google Cloud Storage: {body_string}",);

            return Err(err_gcs(format!(
                "Start upload failed with {} status",
                response.status()
            )));
        }

        Ok(url::Url::parse(
            response
                .headers()
                .get("Location")
                .ok_or_else(|| err_gcs("Missing Location header".to_string()))?
                .to_str()?,
        )?)
    }

    async fn upload_data(&mut self, url: &url::Url, part: BufferPart) -> Result<usize> {
        let mut response = retriable_request(&self.backoff_strategy, &mut self.client, || {
            let request = Request::builder()
                .method(Method::PUT)
                .uri(url.to_string())
                .header(
                    header::CONTENT_RANGE,
                    format!(
                        "bytes {}-{}/*",
                        part.start(),
                        // -1 on the end is here, because Content-Range is inclusive and our range is exclusive
                        part.end() - 1
                    ),
                )
                .header(header::USER_AGENT, "Tremor")
                .header(header::ACCEPT, "*/*")
                .body(Body::from(part.data.clone()))?;

            Ok(request)
        })
        .await?;

        if response.status().is_server_error() {
            let mut data: Vec<u8> = Vec::new();
            while let Some(chunk) = response.data().await.transpose()? {
                data.extend_from_slice(&chunk);
            }
            let body_string = String::from_utf8(data)?;
            error!("Error from Google Cloud Storage: {body_string}",);
            return Err(err_gcs(format!(
                "Start upload failed with status {}",
                response.status()
            )));
        }

        let raw_range = response
            .headers()
            .get(header::RANGE)
            .ok_or_else(|| err_gcs("No range header"))?;
        let raw_range = raw_range.to_str()?;

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
            let request = Request::builder()
                .method(Method::DELETE)
                .uri(url.to_string())
                .body(Body::empty())?;
            Ok(request)
        })
        .await?;
        // dont ask me, see: https://cloud.google.com/storage/docs/performing-resumable-uploads#cancel-upload
        let status = response.status().as_u16();
        if status == 499 || status == 204 {
            Ok(())
        } else {
            let mut data: Vec<u8> = Vec::new();
            while let Some(chunk) = response.data().await.transpose()? {
                data.extend_from_slice(&chunk);
            }
            let body_string = String::from_utf8(data)?;

            error!("Error from Google Cloud Storage while cancelling an upload: {body_string}",);
            Err(err_gcs(format!(
                "Delete upload failed with status {}",
                response.status()
            )))
        }
    }

    async fn finish_upload(&mut self, url: &url::Url, part: BufferPart) -> Result<()> {
        let mut response = retriable_request(&self.backoff_strategy, &mut self.client, || {
            let request = Request::builder()
                .method(Method::PUT)
                .uri(url.to_string())
                .header(
                    header::CONTENT_RANGE,
                    format!(
                        "bytes {}-{}/{}",
                        part.start(),
                        // -1 on the end is here, because Content-Range is inclusive
                        part.end() - 1,
                        part.end()
                    ),
                )
                .body(Body::from(part.data.clone()))?;

            Ok(request)
        })
        .await?;

        if response.status().is_server_error() {
            let mut data: Vec<u8> = Vec::new();
            while let Some(chunk) = response.data().await.transpose()? {
                data.extend_from_slice(&chunk);
            }
            let body_string = String::from_utf8(data)?;

            error!("Error from Google Cloud Storage: {body_string}");

            return Err(err_gcs(format!(
                "Finish upload failed with status {}",
                response.status()
            )));
        }

        Ok(())
    }
}

impl<TClient: HttpClientTrait, TBackoffStrategy: BackoffStrategy>
    DefaultClient<TClient, TBackoffStrategy>
{
    // allow is required to not produce warnings in test mode
    #[allow(clippy::unnecessary_wraps, unused_variables)]
    pub fn new(
        client: TClient,
        backoff_strategy: TBackoffStrategy,
        token: &TokenSrc,
    ) -> Result<Self> {
        Ok(Self {
            #[cfg(not(test))]
            token: token.to_token()?,
            client,
            backoff_strategy,
        })
    }

    #[cfg(test)]
    fn create_upload_start_request(
        url: &Url<HttpsDefaults>,
        file: &ObjectId,
    ) -> Result<Request<Body>> {
        let url = format!(
            "{}/b/{}/o?name={}&uploadType=resumable",
            url,
            file.bucket(),
            file.name()
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri(url)
            .body(Body::empty())?;

        Ok(request)
    }

    #[cfg(not(test))]
    fn create_upload_start_request(
        token: &Token,
        url: &Url<HttpsDefaults>,
        file: &ObjectId,
    ) -> Result<Request<Body>> {
        let url = format!(
            "{}/b/{}/o?name={}&uploadType=resumable",
            url,
            file.bucket(),
            file.name()
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri(url)
            .header(
                hyper::header::AUTHORIZATION,
                token.header_value()?.to_string(),
            )
            .body(Body::empty())?;

        Ok(request)
    }
}

pub(crate) fn create_client(_connect_timeout: Duration) -> GcsHttpClient {
    let https = HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();

    hyper::Client::builder().build(https)
}

#[async_trait::async_trait]
pub(crate) trait HttpClientTrait: Send + Sync {
    async fn request(&self, req: hyper::Request<Body>) -> Result<hyper::Response<Body>>;
}

#[async_trait::async_trait]
impl HttpClientTrait for GcsHttpClient {
    async fn request(&self, req: Request<Body>) -> Result<Response<Body>> {
        Ok(self.request(req).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::{Debug, Formatter};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    pub(crate) struct MockHttpClient {
        pub handle_request: Box<dyn Fn(Request<Body>) -> Result<Response<Body>> + Send + Sync>,
        pub simulate_failure: Arc<AtomicBool>,
        pub simulate_transport_failure: Arc<AtomicBool>,
    }

    impl Debug for MockHttpClient {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "<mock>")
        }
    }

    #[async_trait::async_trait]
    impl HttpClientTrait for MockHttpClient {
        async fn request(&self, req: Request<Body>) -> Result<Response<Body>> {
            if self
                .simulate_transport_failure
                .swap(false, Ordering::AcqRel)
            {
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())?);
            }

            if self.simulate_failure.swap(false, Ordering::AcqRel) {
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())?);
            }

            (self.handle_request)(req)
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn can_bucket_exists() -> Result<()> {
        let client = MockHttpClient {
            handle_request: Box::new(|req| {
                assert_eq!(req.uri().path(), "/b/snot");
                assert_eq!(req.method(), Method::GET);

                let response = Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::empty())?;
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
        let bucket_exists = api_client
            .bucket_exists(&Url::parse("http://example.com")?, "snot")
            .await?;
        assert!(bucket_exists);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn can_start_upload() -> Result<()> {
        let client = MockHttpClient {
            handle_request: Box::new(|req| {
                assert_eq!(req.uri().path(), "/upload/b/bucket/o");
                assert_eq!(
                    req.uri().query().unwrap_or_default(),
                    "name=somefile&uploadType=resumable".to_string()
                );

                let response = Response::builder()
                    .status(StatusCode::PERMANENT_REDIRECT)
                    .header("Location", "http://example.com/upload_session")
                    .body(Body::empty())?;
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
                ObjectId::new("bucket", "somefile"),
            )
            .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn can_delete_upload() -> Result<()> {
        let client = MockHttpClient {
            handle_request: Box::new(|req| {
                assert_eq!(req.method(), Method::DELETE);
                assert_eq!(req.uri().path(), "/upload_session");

                let response = Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(Body::empty())?;
                Ok(response)
            }),
            simulate_failure: Arc::new(AtomicBool::new(true)),
            simulate_transport_failure: Arc::new(AtomicBool::new(true)),
        };
        let mut api_client = DefaultClient::new(
            client,
            ExponentialBackoffRetryStrategy {
                max_retries: 3,
                base_sleep_time: Duration::from_nanos(1),
            },
            &TokenSrc::dummy(),
        )?;
        api_client
            .delete_upload(
                &url::Url::parse("http://example.com/upload_session")
                    .expect("static url did not parse"),
            )
            .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn can_upload_data() -> Result<()> {
        let client = MockHttpClient {
            handle_request: Box::new(|mut req| {
                let mut body: Vec<u8> = Vec::new();
                while let Some(chunk) = futures::executor::block_on(req.data()).transpose()? {
                    body.extend_from_slice(&chunk);
                }

                assert_eq!(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], body);
                assert_eq!(
                    req.headers()
                        .get(header::CONTENT_RANGE)
                        .and_then(|h| h.to_str().ok()),
                    Some("bytes 0-9/*")
                );

                let response = Response::builder()
                    .status(StatusCode::OK)
                    .header("Range", "bytes=0-10")
                    .body(Body::empty())?;
                Ok(response)
            }),
            simulate_failure: Arc::new(AtomicBool::new(true)),
            simulate_transport_failure: Arc::new(AtomicBool::new(true)),
        };
        let session_url = url::Url::parse("https://example.com/session")?;
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

    #[tokio::test(flavor = "multi_thread")]
    pub async fn upload_data_fails_on_failed_request() -> Result<()> {
        let client = MockHttpClient {
            handle_request: Box::new(|_req| {
                Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())?)
            }),
            simulate_failure: Arc::new(AtomicBool::new(true)),
            simulate_transport_failure: Arc::new(AtomicBool::new(true)),
        };
        let session_url = url::Url::parse("https://example.com/session")?;
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

    #[tokio::test(flavor = "multi_thread")]
    pub async fn can_finish_upload() -> Result<()> {
        let client = MockHttpClient {
            handle_request: Box::new(|req| {
                assert_eq!(
                    req.headers()
                        .get(header::CONTENT_RANGE)
                        .and_then(|h| h.to_str().ok()),
                    Some("bytes 10-12/13")
                );

                Ok(Response::new(Body::empty()))
            }),
            simulate_failure: Arc::new(AtomicBool::new(true)),
            simulate_transport_failure: Arc::new(AtomicBool::new(true)),
        };

        let session_url = url::Url::parse("https://example.com/session")?;
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

    #[tokio::test(flavor = "multi_thread")]
    async fn retries_on_server_error() -> Result<()> {
        let request_handled = Arc::new(AtomicBool::new(false));
        let request_handled_clone = request_handled.clone();

        let response = retriable_request(
            &ExponentialBackoffRetryStrategy::new(3, Duration::from_nanos(1)),
            &mut MockHttpClient {
                handle_request: Box::new(move |_req| {
                    request_handled_clone.swap(true, Ordering::Acquire);

                    Ok(Response::new(Body::empty()))
                }),
                simulate_failure: Arc::new(AtomicBool::new(true)),
                simulate_transport_failure: Arc::default(),
            },
            || {
                Ok(Request::builder()
                    .method(Method::GET)
                    .uri("http://example.com")
                    .body(Body::empty())?)
            },
        )
        .await?;

        assert!(request_handled.load(Ordering::Acquire));
        assert_eq!(response.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fails_when_retries_are_exhausted() {
        let response = retriable_request(
            &ExponentialBackoffRetryStrategy::new(3, Duration::from_nanos(1)),
            &mut MockHttpClient {
                handle_request: Box::new(move |_req| {
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::empty())?)
                }),
                simulate_failure: Arc::new(AtomicBool::new(true)),
                simulate_transport_failure: Arc::default(),
            },
            || {
                Ok(Request::builder()
                    .method(Method::GET)
                    .uri("http://example.com")
                    .body(Body::empty())?)
            },
        )
        .await;

        assert!(response.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retries_on_request_creation_failure() -> Result<()> {
        let request_handled = Arc::new(AtomicBool::new(false));
        let request_handled_clone = request_handled.clone();

        let response = retriable_request(
            &ExponentialBackoffRetryStrategy::new(3, Duration::from_nanos(1)),
            &mut MockHttpClient {
                handle_request: Box::new(move |_req| Ok(Response::new(Body::empty()))),
                simulate_failure: Arc::new(AtomicBool::new(true)),
                simulate_transport_failure: Arc::default(),
            },
            || {
                if !request_handled_clone.swap(true, Ordering::Acquire) {
                    return Err("boo".into());
                }

                Ok(Request::builder()
                    .method(Method::GET)
                    .uri("http://example.com")
                    .body(Body::empty())?)
            },
        )
        .await?;

        assert!(request_handled.load(Ordering::Acquire));
        assert_eq!(response.status(), StatusCode::OK);
        Ok(())
    }
}
