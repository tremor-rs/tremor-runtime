use crate::connectors::impls::gcs::chunked_buffer::BufferPart;
use crate::connectors::prelude::{ContraflowData, ErrorKind, Result, Url};
use crate::connectors::utils::url::HttpsDefaults;
use async_std::task::sleep;
#[cfg(not(test))]
use gouth::Token;
use http_client::HttpClient;
use http_types::{Method, Request};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub(crate) struct HttpTaskRequest {
    pub command: HttpTaskCommand,
    pub contraflow_data: Option<ContraflowData>,
    pub start: u64,
}

#[derive(Debug, PartialEq)]
pub(crate) enum HttpTaskCommand {
    FinishUpload { file: FileId, data: BufferPart },
    StartUpload { file: FileId },
    UploadData { file: FileId, data: BufferPart },
}

#[derive(Hash, PartialEq, Eq, Debug)]
pub(crate) struct FileId {
    pub bucket: String,
    pub name: String,
}

impl FileId {
    pub fn new(bucket: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            bucket: bucket.into(),
        }
    }
}

pub(crate) async fn handle_http_command(
    done_until: Arc<AtomicUsize>,
    client: &impl HttpClient,
    url: &Url<HttpsDefaults>,
    #[cfg(not(test))] token: &Token,
    sessions_per_file: &mut HashMap<FileId, url::Url>,
    command: HttpTaskCommand,
) -> Result<()> {
    match command {
        HttpTaskCommand::FinishUpload { file, data } => {
            finish_upload(client, sessions_per_file, file, data).await
        }
        HttpTaskCommand::StartUpload { file } => {
            start_upload(
                client,
                url,
                #[cfg(not(test))]
                token,
                sessions_per_file,
                file,
            )
            .await
        }
        HttpTaskCommand::UploadData { file, data } => {
            upload_data(done_until, client, sessions_per_file, file, data).await
        }
    }
}

async fn upload_data(
    done_until: Arc<AtomicUsize>,
    client: &impl HttpClient,
    sessions_per_file: &mut HashMap<FileId, url::Url>,
    file: FileId,
    data: BufferPart,
) -> Result<()> {
    let mut response = None;
    for i in 0..3 {
        let session_url =
            sessions_per_file
                .get(&file)
                .ok_or(ErrorKind::GoogleCloudStorageError(
                    "No session URL is available",
                ))?;
        let mut request = Request::new(Method::Put, session_url.clone());

        request.insert_header(
            "Content-Range",
            format!(
                "bytes {}-{}/*",
                data.start,
                // -1 on the end is here, because Content-Range is inclusive and our range is exclusive
                data.start + data.len() - 1
            ),
        );
        request.insert_header("User-Agent", "Tremor");
        request.insert_header("Accept", "*/*");
        request.set_body(data.data.clone());

        match client.send(request).await {
            Ok(request) => response = Some(request),
            Err(e) => {
                warn!("Failed to send a request to GCS: {}", e);

                sleep(Duration::from_millis(25u64 * 2u64.pow(i))).await;
                continue;
            }
        }

        if let Some(response) = response.as_mut() {
            if !response.status().is_server_error() && response.header("range").is_some() {
                break;
            }

            error!(
                "Error from Google Cloud Storage: {}",
                response.body_string().await?
            );

            sleep(Duration::from_millis(25u64 * 2u64.pow(i))).await;
        }
    }

    if let Some(mut response) = response {
        if response.status().is_server_error() {
            error!(
                "Error from Google Cloud Storage: {}",
                response.body_string().await?
            );
            return Err("Received server errors from Google Cloud Storage".into());
        }

        if let Some(raw_range) = response.header("range") {
            let raw_range = raw_range
                .get(0)
                .ok_or(ErrorKind::GoogleCloudStorageError(
                    "Missing Range header value",
                ))?
                .as_str();

            // Range format: bytes=0-262143
            let range_end = &raw_range
                .get(
                    raw_range
                        .find('-')
                        .ok_or(ErrorKind::GoogleCloudStorageError(
                            "Did not find a - in the Range header",
                        ))?
                        + 1..,
                )
                .ok_or(ErrorKind::GoogleCloudStorageError(
                    "Unable to get the end of the Range",
                ))?;

            // NOTE: The data can only be thrown away to the point of the end of the Range header,
            // since google can persist less than we send in our request.
            //
            // see: https://cloud.google.com/storage/docs/performing-resumable-uploads#chunked-upload
            done_until.store(range_end.parse()?, Ordering::Release);
        } else {
            return Err("No range header".into());
        }
    } else {
        return Err("no response from GCS".into());
    }

    Ok(())
}

async fn start_upload(
    client: &impl HttpClient,
    url: &Url<HttpsDefaults>,
    #[cfg(not(test))] token: &Token,
    sessions_per_file: &mut HashMap<FileId, url::Url>,
    file: FileId,
) -> Result<()> {
    for i in 0..3 {
        let url = url::Url::parse(&format!(
            "{}/b/{}/o?name={}&uploadType=resumable",
            url, file.bucket, file.name
        ))?;
        #[cfg(not(test))]
        let mut request = Request::new(Method::Post, url);
        #[cfg(test)]
        let request = Request::new(Method::Post, url);
        #[cfg(not(test))]
        {
            request.insert_header("Authorization", token.header_value()?.to_string());
        }

        let response = client.send(request).await;

        if let Ok(mut response) = response {
            if !response.status().is_server_error() {
                sessions_per_file.insert(
                    file,
                    url::Url::parse(
                        response
                            .header("Location")
                            .ok_or(ErrorKind::GoogleCloudStorageError(
                                "Missing Location header",
                            ))?
                            .get(0)
                            .ok_or(ErrorKind::GoogleCloudStorageError(
                                "Missing Location header value",
                            ))?
                            .as_str(),
                    )?,
                );

                break;
            }

            error!(
                "Error from Google Cloud Storage: {}",
                response.body_string().await?
            );
        }

        sleep(Duration::from_millis(25u64 * 2u64.pow(i))).await;
        continue;
    }

    Ok(())
}

async fn finish_upload(
    client: &impl HttpClient,
    sessions_per_file: &mut HashMap<FileId, url::Url>,
    file: FileId,
    data: BufferPart,
) -> Result<()> {
    let session_url: url::Url =
        sessions_per_file
            .remove(&file)
            .ok_or(ErrorKind::GoogleCloudStorageError(
                "No session URL is available",
            ))?;

    for i in 0..3 {
        let mut request = Request::new(Method::Put, session_url.clone());

        request.insert_header(
            "Content-Range",
            format!(
                "bytes {}-{}/{}",
                data.start,
                // -1 on the end is here, because Content-Range is inclusive
                data.start + data.len() - 1,
                data.start + data.len()
            ),
        );

        request.set_body(data.data.clone());

        let response = client.send(request).await;

        if let Ok(mut response) = response {
            if !response.status().is_server_error() {
                break;
            }
            error!(
                "Error from Google Cloud Storage: {}",
                response.body_string().await?
            );
        }

        sleep(Duration::from_millis(25u64 * 2u64.pow(i))).await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task::block_on;
    use http_types::{Error, Response, StatusCode};
    use std::fmt::{Debug, Formatter};
    use std::sync::atomic::AtomicBool;

    pub struct MockHttpClient {
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
        let mut sessions_per_file = HashMap::new();
        start_upload(
            &client,
            &Url::parse("http://example.com/upload").unwrap(),
            &mut sessions_per_file,
            FileId::new("bucket", "somefile"),
        )
        .await?;

        assert_eq!(
            Some(&url::Url::parse("http://example.com/upload_session").unwrap()),
            sessions_per_file.get(&FileId::new("bucket", "somefile"))
        );

        Ok(())
    }

    #[async_std::test]
    pub async fn can_upload_data() -> Result<()> {
        let done_until = Arc::new(AtomicUsize::new(0));
        let mock_client = MockHttpClient {
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
        let mut sessions_per_file = HashMap::new();
        sessions_per_file.insert(
            FileId::new("bucket", "my_file"),
            url::Url::parse("https://example.com/session").unwrap(),
        );

        upload_data(
            done_until.clone(),
            &mock_client,
            &mut sessions_per_file,
            FileId::new("bucket", "my_file"),
            BufferPart {
                data: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                start: 0,
            },
        )
        .await?;

        Ok(())
    }

    #[async_std::test]
    pub async fn can_finish_upload() -> Result<()> {
        let mut client = MockHttpClient {
            config: Default::default(),
            handle_request: Box::new(|req| {
                assert_eq!(req.header("Content-Range").unwrap()[0], "bytes 10-12/13");

                Ok(Response::new(StatusCode::Ok))
            }),
            simulate_failure: Arc::new(AtomicBool::new(true)),
            simulate_transport_failure: Arc::new(AtomicBool::new(true)),
        };

        let mut sessions_per_file = HashMap::new();
        sessions_per_file.insert(
            FileId::new("somebucket", "somefile"),
            url::Url::parse("https://example.com/session").unwrap(),
        );
        finish_upload(
            &mut client,
            &mut sessions_per_file,
            FileId::new("somebucket", "somefile"),
            BufferPart {
                data: vec![1, 2, 3],
                start: 10,
            },
        )
        .await?;

        Ok(())
    }
}
