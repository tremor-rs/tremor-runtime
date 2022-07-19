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

pub(crate) struct HttpTaskRequest {
    pub command: HttpTaskCommand,
    pub contraflow_data: Option<ContraflowData>,
    pub start: u64,
}

pub(crate) enum HttpTaskCommand {
    FinishUpload { file: FileId, data: BufferPart },
    StartUpload { file: FileId },
    UploadData { file: FileId, data: BufferPart },
}

#[derive(Hash, PartialEq, Eq)]
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
                data.start + data.length - 1
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
                data.start + data.length - 1,
                data.start + data.length
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
