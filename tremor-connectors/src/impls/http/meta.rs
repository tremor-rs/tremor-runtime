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

use crate::{
    impls::http::{client, utils::Body, utils::RequestId},
    sink::EventSerializer,
    utils::mime::MimeCodecMap,
};
use either::Either;
use http::{
    header::{self, HeaderName, ToStrError},
    HeaderMap, Uri,
};
use http_body_util::BodyStream;
use hyper::{header::HeaderValue, Method, Request, Response};
use mime::Mime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tremor_config::NameWithConfig;
use tremor_system::qsize;
use tremor_value::prelude::*;

use super::utils::body_from_bytes;

/// HTTP Connector Errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid HTTP Method
    #[error("Invalid HTTP Method")]
    InvalidMethod,
    /// Invalid HTTP URL
    #[error("Invalid HTTP URL")]
    InvalidUrl,
    /// Request already consumed
    #[error("Request already consumed")]
    RequestAlreadyConsumed,
    /// Response already consumed
    #[error("Response already consumed")]
    ResponseAlreadyConsumed,
}

/// Utility for building an HTTP request from a possibly batched event
/// and some configuration values
pub(crate) struct HttpRequestBuilder {
    request_id: RequestId,
    builder: Option<http::request::Builder>,
    //request: Option<hyper::Request<BodyStream<Body>>>,
    chunk_tx: Sender<Vec<u8>>,
    chunk_rx: Receiver<Vec<u8>>,
    codec_overwrite: Option<NameWithConfig>,
}

#[derive(Clone)]
pub(crate) struct HeaderValueValue<'v> {
    finished: bool,
    idx: usize,
    v: &'v Value<'v>,
}

impl<'v> HeaderValueValue<'v> {
    pub fn new(v: &'v Value<'v>) -> Self {
        Self {
            idx: 0,
            finished: false,
            v,
        }
    }
}

impl<'v> Iterator for HeaderValueValue<'v> {
    type Item = HeaderValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            None
        } else if let Some(a) = self.v.as_array() {
            loop {
                if a.is_empty() {
                    self.finished = true;
                    return None;
                }
                let v = a.get(self.idx)?;
                self.idx += 1;
                if let Some(v) = v.as_str().and_then(|v| HeaderValue::from_str(v).ok()) {
                    return Some(v);
                } else if let Ok(v) = HeaderValue::from_str(&v.encode()) {
                    return Some(v);
                }
            }
        } else if let Some(v) = self.v.as_str().and_then(|v| HeaderValue::from_str(v).ok()) {
            self.finished = true;
            Some(v)
        } else if let Ok(v) = HeaderValue::from_str(&self.v.encode()) {
            self.finished = true;
            Some(v)
        } else {
            self.finished = true;
            None
        }
    }
}

// TODO: do some deduplication with SinkResponse
impl HttpRequestBuilder {
    pub(super) fn new(
        request_id: RequestId,
        meta: Option<&Value>,
        codec_map: &MimeCodecMap,
        config: &client::Config,
    ) -> anyhow::Result<Self> {
        let request_meta = meta.get("request");
        let method = if let Some(method_v) = request_meta.get("method") {
            Method::from_bytes(method_v.as_bytes().ok_or(Error::InvalidMethod)?)?
        } else {
            config.method.0.clone()
        };
        let uri: Uri = if let Some(url_v) = request_meta.get("url") {
            url_v.as_str().ok_or(Error::InvalidUrl)?.parse()?
        } else {
            config.url.to_string().parse()?
        };
        let mut request = Request::builder().method(method).uri(uri);

        // first insert config headers
        for (config_header_name, config_header_values) in &config.headers {
            match &config_header_values.0 {
                Either::Left(config_header_values) => {
                    for header_value in config_header_values {
                        request =
                            request.header(config_header_name.as_str(), header_value.as_str());
                    }
                }
                Either::Right(header_value) => {
                    request = request.header(config_header_name.as_str(), header_value.as_str());
                }
            }
        }
        let headers = request_meta.get("headers");

        // build headers
        if let Some(headers) = headers.as_object() {
            for (name, values) in headers {
                let name = HeaderName::from_bytes(name.as_bytes())?;
                for value in HeaderValueValue::new(values) {
                    request = request.header(&name, value);
                }
            }
        }

        let header_content_type = content_type(request.headers_ref())?;

        let (codec_overwrite, content_type) =
            consolidate_mime(header_content_type.clone(), codec_map);

        // set the content type if it is not set yet
        if header_content_type.is_none() {
            if let Some(ct) = content_type {
                request = request.header(header::CONTENT_TYPE, ct.to_string());
            }
        }
        // handle AUTH
        if let Some(auth_header) = config.auth.as_header_value()? {
            request = request.header(hyper::header::AUTHORIZATION, auth_header);
        }

        let (chunk_tx, chunk_rx) = channel::<Vec<u8>>(qsize());

        // extract headers
        // determine content-type, override codec and chunked encoding
        Ok(Self {
            request_id,
            builder: Some(request),
            chunk_tx,
            chunk_rx,
            codec_overwrite,
        })
    }

    pub(super) async fn append<'event>(
        &mut self,
        value: &'event Value<'event>,
        meta: &'event Value<'event>,
        ingest_ns: u64,
        serializer: &mut EventSerializer,
    ) -> anyhow::Result<()> {
        let chunks = serializer
            .serialize_for_stream_with_codec(
                value,
                meta,
                ingest_ns,
                self.request_id.get(),
                self.codec_overwrite.as_ref(),
            )
            .await?;
        self.append_data(chunks).await
    }

    async fn append_data(&mut self, chunks: Vec<Vec<u8>>) -> anyhow::Result<()> {
        for chunk in chunks {
            self.chunk_tx.send(chunk).await?;
        }
        Ok(())
    }

    pub(super) fn take_request(&mut self) -> Result<Request<BodyStream<Body>>, Error> {
        let mut data = Vec::new();

        // drain the channel
        while let Ok(chunk) = self.chunk_rx.try_recv() {
            data.extend(chunk);
        }

        let builder = self.builder.take().ok_or(Error::RequestAlreadyConsumed)?;
        let body_stream = BodyStream::new(body_from_bytes(data));
        let request = builder
            .body(body_stream)
            .map_err(|_| Error::RequestAlreadyConsumed)?;
        Ok(request)
    }
    /// Finalize and send the response.
    /// In the chunked case we have already sent it before.
    ///
    /// After calling this function this instance shouldn't be used anymore
    pub(super) async fn finalize(
        &mut self,
        serializer: &mut EventSerializer,
    ) -> anyhow::Result<()> {
        // finalize the stream
        let rest = serializer.finish_stream(self.request_id.get())?;
        self.append_data(rest).await
    }
}
/// Extract the content type from the headers
/// # Errors
///   if the content type is not a valid string
pub fn content_type(headers: Option<&HeaderMap>) -> anyhow::Result<Option<Mime>> {
    if let Some(headers) = headers {
        let header_content_type = headers
            .get(hyper::header::CONTENT_TYPE)
            .map(|v| String::from_utf8(v.as_bytes().to_vec()))
            .transpose()?
            .map(|v| v.parse())
            .transpose()?;
        Ok(header_content_type)
    } else {
        Ok(None)
    }
}

/// extract content-type and thus possible codec overwrite only from first element
/// precedence:
///  1. from headers meta
///  4. from the `*/*` codec if one was supplied
///  3. fall back to application/octet-stream if codec doesn't provide a mime-type
pub(crate) fn consolidate_mime(
    header_content_type: Option<Mime>,
    codec_map: &MimeCodecMap,
) -> (Option<NameWithConfig>, Option<Mime>) {
    let codec_overwrite = if let Some(header_content_type) = &header_content_type {
        codec_map.get_codec_name(header_content_type.essence_str())
    } else {
        codec_map.get_codec_name("*/*")
    }
    .cloned();
    let codec_content_type = codec_overwrite
        .as_ref()
        .and_then(|codec| codec_map.get_mime_type(codec.name.as_str()))
        .and_then(|mime| mime.parse::<Mime>().ok());
    let content_type = Some(
        header_content_type
            .or(codec_content_type)
            .unwrap_or(mime::APPLICATION_OCTET_STREAM),
    );
    (codec_overwrite, content_type)
}

fn extract_headers(headers: &HeaderMap) -> Result<Value<'static>, ToStrError> {
    headers
        .keys()
        .map(|name| {
            Ok((
                name.to_string(),
                // a header name has the potential to take multiple values:
                // https://tools.ietf.org/html/rfc7230#section-3.2.2
                headers
                    .get_all(name)
                    .iter()
                    .map(|v| Ok(Value::from(v.to_str()?.to_string())))
                    .collect::<Result<Value<'static>, ToStrError>>()?,
            ))
        })
        .collect::<Result<Value<'static>, ToStrError>>()
}
/// Extract request metadata
pub(super) fn extract_request_meta<T>(
    request: &Request<T>,
    scheme: &'static str,
) -> Result<Value<'static>, ToStrError> {
    // collect header values into an array for each header
    let headers: Value<'static> = extract_headers(request.headers())?;

    Ok(literal!({
        "headers": headers,
        "method": request.method().to_string(),
        "protocol": Value::from(scheme),
        "uri": request.uri().to_string(),
        "version": format!("{:?}", request.version()),
    }))
}

/// extract response metadata
pub(super) fn extract_response_meta<B>(
    response: &Response<B>,
) -> Result<Value<'static>, ToStrError> {
    // collect header values into an array for each header
    let headers = extract_headers(response.headers())?;

    let mut meta = Value::object_with_capacity(3);
    meta.try_insert("status", response.status().as_u16());
    meta.try_insert("headers", headers);
    meta.try_insert("version", format!("{:?}", response.version()));
    Ok(meta)
}

#[cfg(test)]
mod test {
    use crate::CodecReq;
    use crate::ConnectorType;
    use tremor_common::alias;
    use tremor_config::Impl;

    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    async fn builder() -> anyhow::Result<()> {
        let request_id = RequestId::new(42);
        let meta = None;
        let codec_map = MimeCodecMap::default();
        let c = literal!({"headers": {
            "cake": ["black forst", "cheese"],
            "pie": "key lime"
        }});
        let mut s = EventSerializer::new(
            None,
            CodecReq::Optional("json"),
            vec![],
            &ConnectorType("http".into()),
            &alias::Connector::new("flow", "http"),
        )?;
        let config = client::Config::new(&c)?;

        let mut b = HttpRequestBuilder::new(request_id, meta, &codec_map, &config)?;

        let r = b.take_request()?;
        b.finalize(&mut s).await?;
        assert_eq!(r.headers().get_all("pie").iter().count(), 1);
        assert_eq!(r.headers().get_all("cake").iter().count(), 2);
        Ok(())
    }
}
