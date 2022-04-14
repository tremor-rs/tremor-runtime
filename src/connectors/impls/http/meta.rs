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

use super::client;
use super::utils::{FixedBodyReader, RequestId, StreamingBodyReader};
use crate::connectors::{prelude::*, utils::mime::MimeCodecMap};
use async_std::channel::{unbounded, Sender};
use http_types::{
    headers::{self, HeaderValue},
    mime::BYTE_STREAM,
    Method, Mime, Request,
};
use std::str::FromStr;
use tremor_value::Value;
use value_trait::ValueAccess;

// NOTE Initial attempt at extracting request/response handling for reuse in other HTTP based connectors
// TODO Extract headers into separate struct, separate out Config which is http client connector specific
// TODO Leverage default / static headers in surf?
// TODO Consider alternate httpc than surf due to issues with HTTP 1.0 backwards compatibility
// TODO Deterministic method of setting content-type
// TODO PBT's harness for full request/response client/server request/response lifecycle to test http conformance

/// Body data enum for chunked or non-chunked data
pub(crate) enum BodyData {
    Data(Vec<Vec<u8>>),
    Chunked(Sender<Vec<u8>>),
}

/*
pub(crate) struct HttpResponseMeta {}

pub(crate) enum ResponseEventCont {
    Valid(Vec<SourceReply>),
    CodecError,
}

impl HttpResponseMeta {
    #[allow(dead_code)]
    pub(crate) fn from_config(_config: &ConnectorConfig, _default_codec: &str) -> Self {
        HttpResponseMeta {}
    }

    /// Conditionally Converts a HTTP response to a tremor event
    async fn body_to_event(
        codec: &mut Box<dyn Codec>,
        preprocessors: &mut Preprocessors,
        origin_uri: &EventOriginUri,
        mut ingest_ns: u64,
        meta: &Value<'static>,
        response: &mut SurfResponse,
    ) -> Result<ResponseEventCont> {
        let response_bytes = response.body_bytes().await;
        let response_bytes = response_bytes?;
        let pp = preprocess(preprocessors, &mut ingest_ns, response_bytes, "http_client")?;
        let mut events = Vec::with_capacity(pp.len());
        for pp in pp {
            let payload = EventPayload::try_new::<crate::Error, _>(pp, |mut_data| {
                let body = codec.decode(mut_data, nanotime());
                let body = body?.unwrap_or_else(Value::object);
                Ok(ValueAndMeta::from_parts(body, meta.clone()))
            });
            if let Err(e) = payload {
                error!(
                    "Failed to preprocess event - decoding error during processing: {}",
                    e
                );
                return Ok(ResponseEventCont::CodecError);
            }
            events.push(SourceReply::Structured {
                origin_uri: origin_uri.clone(),
                payload: payload?,
                stream: DEFAULT_STREAM_ID,
                port: None,
            });
        }

        Ok(ResponseEventCont::Valid(events))
    }

    // Given a HTTP request, invokes and conditionally returns a response event
    pub(crate) async fn invoke(
        codec: &mut Box<dyn Codec>,
        preprocessors: &mut Preprocessors,
        _postprocessors: &mut Postprocessors,
        request_meta: Value<'static>,
        origin_uri: &EventOriginUri,
        client: surf::Client,
        request: SurfRequest,
    ) -> Result<ResponseEventCont> {
        let mut origin_uri = origin_uri.clone();
        origin_uri.host = "snot".to_string(); // client.host;

        let sent_at = nanotime();
        let response = client.send(request).await;

        let mut response = response?;
        let status = response.status();
        let status_code: u16 = status.into();
        let rcvd_at = nanotime();

        let mut response_meta = Value::object_with_capacity(2);

        // Propagate correlation metadata
        if let Some(correlation) = request_meta.get("correlation") {
            response_meta.insert("correlation", correlation.clone_static())?;
        }

        // Record the round trip time duration for the interaction triggering CB on failure
        // Record HTTP response status code
        let mut meta = literal!({
            "time": rcvd_at - sent_at,
            "status": status_code,
            "request": request_meta,
        });

        // Propagate header values
        let mut headers = Value::object_with_capacity(8);
        {
            for (hn, hv) in response.iter() {
                let hv: Value = hv
                    .iter()
                    .map(ToString::to_string)
                    .map(Value::from)
                    .collect();
                headers.insert(hn.to_string(), hv)?;
            }
        }
        response_meta.insert("headers", headers)?;

        // Inject response metadata
        meta.insert("response", response_meta)?;

        let event = Self::body_to_event(
            codec,
            preprocessors,
            &origin_uri,
            rcvd_at,
            &meta,
            &mut response,
        )
        .await;

        event
    }
}
*/
pub(crate) struct HttpRequestBuilder {
    request_id: RequestId,
    request: Option<Request>,
    body_data: BodyData,
    codec_overwrite: Option<String>,
}

impl HttpRequestBuilder {
    pub(super) fn new(
        request_id: RequestId,
        meta: Option<&Value>,
        codec_map: &MimeCodecMap,
        config: &client::Config,
        configured_codec: &String,
    ) -> Result<Self> {
        let request_meta = meta.get("request");
        let method = if let Some(method_v) = request_meta.get("method") {
            if let Some(method_str) = method_v.as_str() {
                Method::from_str(method_str)?
            } else {
                return Err("Invalid HTTP Method".into());
            }
        } else {
            config.method.clone()
        };
        let url = if let Some(url_v) = request_meta.get("url") {
            if let Some(url_str) = url_v.as_str() {
                Url::parse(url_str)?
            } else {
                return Err("Invalid HTTP URL".into());
            }
        } else {
            config.url.clone()
        };
        let mut request = Request::new(method, url.url().clone());
        let headers = request_meta.get("headers");
        let chunked_header = headers.get(headers::TRANSFER_ENCODING.as_str());
        let chunked = chunked_header
            .as_array()
            .and_then(|te| te.last())
            .and_then(ValueAccess::as_str)
            .or_else(|| chunked_header.as_str())
            .map_or(false, |te| te == "chunked");

        let content_type_header = headers.get(headers::CONTENT_TYPE.as_str());
        let header_content_type = content_type_header
            .as_array()
            .and_then(|ct| ct.last())
            .and_then(ValueAccess::as_str)
            .or_else(|| content_type_header.as_str())
            .map(ToString::to_string);
        let codec_overwrite = header_content_type
            .as_ref()
            .and_then(|mime_str| Mime::from_str(mime_str).ok())
            .and_then(|codec| codec_map.get_codec_name(codec.essence()))
            // only overwrite the codec if it is different from the configured one
            .filter(|codec| *codec != configured_codec)
            .cloned();
        let codec_content_type: Option<String> = codec_overwrite
            .as_ref()
            .and_then(|codec| codec_map.get_mime_type(codec.as_str()))
            .or_else(|| codec_map.get_mime_type(configured_codec))
            .cloned();

        // extract content-type and thus possible codec overwrite only from first element
        // precedence:
        //  1. from headers meta
        //  2. from overwritten codec
        //  3. from configured codec
        //  4. fall back to application/octet-stream if codec doesn't provide a mime-type
        let content_type = Some(
            header_content_type
                .or(codec_content_type)
                .unwrap_or_else(|| BYTE_STREAM.to_string()),
        );
        // first insert config headers
        for (config_header_name, config_header_values) in &config.headers {
            for header_value in config_header_values {
                request.append_header(config_header_name.as_str(), header_value.as_str());
            }
        }
        // build headers
        if let Some(headers) = headers.as_object() {
            for (name, values) in headers {
                if let Some(header_values) = values.as_array() {
                    let mut v = Vec::with_capacity(header_values.len());
                    for value in header_values {
                        if let Some(header_value) = value.as_str() {
                            v.push(HeaderValue::from_str(header_value)?);
                        }
                    }
                    request.append_header(name.as_ref(), v.as_slice());
                } else if let Some(header_value) = values.as_str() {
                    request.append_header(name.as_ref(), header_value);
                }
            }
        }

        // set the content type if it is not set yet
        if request.content_type().is_none() {
            if let Some(ct) = content_type {
                let mime = Mime::from_str(ct.as_str())?;
                request.set_content_type(mime);
            }
        }
        // handle AUTH
        if let Some(auth_header) = config.auth.header_value()? {
            request.insert_header(headers::AUTHORIZATION, auth_header);
        }

        let body_data = if chunked {
            let (chunk_tx, chunk_rx) = unbounded();
            let chunked_reader = StreamingBodyReader::new(chunk_rx);
            request.set_body(surf::Body::from_reader(chunked_reader, None));
            // chunked encoding and content-length cannot go together
            request.remove_header(headers::CONTENT_LENGTH);
            BodyData::Chunked(chunk_tx)
        } else {
            BodyData::Data(Vec::with_capacity(4))
        };

        // extract headers
        // determine content-type, override codec and chunked encoding
        Ok(Self {
            request_id,
            request: Some(request),
            body_data,
            codec_overwrite,
        })
    }

    pub(super) async fn append<'event>(
        &mut self,
        value: &'event Value<'event>,
        ingest_ns: u64,
        serializer: &mut EventSerializer,
    ) -> Result<()> {
        let chunks = serializer.serialize_for_stream_with_codec(
            value,
            ingest_ns,
            self.request_id.get(),
            self.codec_overwrite.as_ref(),
        )?;
        self.append_data(chunks).await
    }

    async fn append_data(&mut self, mut chunks: Vec<Vec<u8>>) -> Result<()> {
        match &mut self.body_data {
            BodyData::Chunked(tx) => {
                for chunk in chunks {
                    tx.send(chunk).await?;
                }
            }
            BodyData::Data(data) => data.append(&mut chunks),
        }
        Ok(())
    }

    /// Consume self and finalize and send the response.
    /// In the chunked case we have already sent it before.
    pub(super) async fn finalize(
        mut self,
        serializer: &mut EventSerializer,
    ) -> Result<Option<Request>> {
        // finalize the stream
        let rest = serializer.finish_stream(self.request_id.get())?;
        if !rest.is_empty() {
            self.append_data(rest).await?;
        }
        // send response if necessary
        match self.body_data {
            BodyData::Data(data) => {
                // set body
                let reader = FixedBodyReader::new(data);
                let len = reader.len();
                if let Some(req) = self.request.as_mut() {
                    req.set_body(surf::Body::from_reader(reader, Some(len)));
                }
            }
            BodyData::Chunked(tx) => {
                // signal EOF to the reader
                tx.close();
            }
        }
        Ok(self.request.take())
    }

    /// Return the ready request if it is chunked
    pub(super) fn get_chunked_request(&mut self) -> Option<Request> {
        if matches!(self.body_data, BodyData::Chunked(_)) {
            self.request.take()
        } else {
            None
        }
    }
}

/*
#[derive(Debug)]
pub(crate) struct BatchItemMeta {
    endpoint: Url,
    method: Method,
    //    auth: HttpAuth, TODO - consider custom per request auth
    headers: HashMap<String, Vec<HeaderValue>>,
    codec: Box<dyn Codec>,
    correlation: Option<Value<'static>>,
}

impl BatchItemMeta {
    fn to_value(&self) -> Value<'static> {
        let mut headers = HashMap::new();
        for (k, v) in &self.headers {
            let mut values = Vec::new();
            for hv in v {
                values.push(hv.to_string());
            }
            let k = k.as_str();
            headers.insert(Cow::from(k), Value::from(values));
        }
        let headers = Value::Object(Box::new(headers));
        literal!({
            "url": self.endpoint.to_string(),
            "method": self.method.to_string(),
            "headers": headers,
            "codec": self.codec.name(),
            "correlation":  self.correlation.as_ref().map_or(Value::const_null(), Value::clone_static)
                    ,
        })
        .into_static()
    }
}
*/

/*
impl HttpRequestBuilder {
    fn meta_to_header_map<'outer, 'event>(
        from_meta: &Value,
        codec_map: &'outer MimeCodecMap,
        default_codec: &'outer (dyn Codec + 'static),
    ) -> Result<HeaderAndCodec<'outer>>
    where
        'outer: 'event,
    {
        if let Some(from_meta) = from_meta.as_object() {
            Self::to_header_map(from_meta, codec_map, default_codec)
        } else {
            Err(Error::from(
                "Expected header metadata to be record structured error",
            ))
        }
    }

    fn to_header_map<'outer, 'event, T>(
        from_config: &HashMap<T, Value>,
        codec_map: &'outer MimeCodecMap,
        default_codec: &'outer (dyn Codec + 'static),
    ) -> Result<HeaderAndCodec<'outer>>
    where
        'outer: 'event,
        T: ToString,
    {
        let mut to = HashMap::new();
        let mut active_codec = None;
        for (name, values) in from_config.iter() {
            let name = name.to_string();
            let name = name.as_str();
            // Skip content-length
            if "content-length".eq_ignore_ascii_case(name) {
                continue;
            }

            let mut active_values = Vec::new();
            if let Value::Array(value_list) = values {
                for v in value_list {
                    active_values.push(HeaderValue::from_str(&v.to_string())?);
                }
                active_codec = Self::refresh_active_codec(
                    codec_map,
                    name,
                    &active_values,
                    Some(default_codec),
                );
            } else if let Value::String(flat) = values {
                active_values.push(HeaderValue::from_str(flat)?);
            } else {
                // NOTE - assumes headers set in metadata are of the form:
                // * { "string": "string" } -> its a flat string value
                // * { "string": [ ... ]} -> its a list of stringifiable values
                // * { "string": <other> } -> we serialize as a JSON string
                let value = values.to_string();
                warn!(
                    "Coercing header value to JSON stringified form {}: {}",
                    name, value
                );
                active_values.push(HeaderValue::from_str(&value)?);
            }
            to.insert(name.to_string(), active_values);
        }

        if let Some(active_codec) = active_codec {
            Ok((to, active_codec))
        } else {
            Ok((to, default_codec))
        }
    }

    fn per_batch_item_overrides(&mut self, meta: &Value) -> Result<BatchItemMeta> {
        if let Some(overrides) = meta.get("request") {
            let active_endpoint = overrides
                .get_str("url")
                .map_or_else(|| Ok(self.endpoint.clone()), Url::parse)?;
            let active_method = overrides
                .get_str("method")
                .map_or_else(|| Ok(self.method), Method::from_str)?;
            let active_correlation = overrides.get("correlation").map(Value::clone_static);
            let (mut active_headers, active_codec) = overrides.get("headers").map_or_else(
                || Ok((self.headers.clone(), self.codec.as_ref())),
                |x| Self::meta_to_header_map(x, &self.codec_map, self.codec.as_ref()),
            )?;

            let active_auth = Auth::from(overrides.get("auth"));

            if let Ok(Some(header_value)) = active_auth.header_value() {
                active_headers.insert(
                    "authorization".to_string(),
                    vec![HeaderValue::from_str(&header_value)?],
                );
            }

            Ok(BatchItemMeta {
                codec: active_codec.boxed_clone(),
                endpoint: active_endpoint,
                method: active_method,
                //                auth: active_auth,
                headers: active_headers,
                correlation: active_correlation,
            })
        } else {
            Ok(BatchItemMeta {
                codec: self.codec.boxed_clone(),
                endpoint: self.endpoint.clone(),
                method: self.method,
                //                auth: self.auth.clone(),
                headers: self.headers.clone(),
                correlation: None,
            })
        }
    }

    pub(crate) fn process(
        &mut self,
        event: &Event,
        alias: &str,
    ) -> Result<(SurfRequest, Value<'static>)> {
        let mut body: Vec<u8> = vec![];

        for (chunk_data, _) in event.value_meta_iter() {
            let encoded = active_meta.codec.encode(chunk_data)?;
            let processed = postprocess(&mut self.postprocessors, event.ingest_ns, encoded, alias)?;
            for mut chunk in processed {
                body.append(&mut chunk);
            }
        }

        Ok((
            Self::into_request(&active_meta, &body),
            active_meta.to_value(),
        ))
    }

    fn into_request(meta: &BatchItemMeta, body: &[u8]) -> SurfRequest {
        debug!("Rest endpoint [{}] chosen", &meta.endpoint);
        let host = match (meta.endpoint.host(), meta.endpoint.port()) {
            (Some(host), Some(port)) => Some(format!("{}:{}", host, port)),
            (Some(host), _) => Some(host.to_string()),
            _ => None,
        };

        let mut request = SurfRequest::new(meta.method, meta.endpoint.url().clone());

        // Build headers from meta - effectively overwrite config headers in case of conflict
        for (k, v) in &meta.headers {
            if "content-type".eq_ignore_ascii_case(k) {
                if let Some(hv) = v.last() {
                    if let Ok(mime) = Mime::from_str(hv.as_str()) {
                        request.set_content_type(mime);
                    }
                }
            }
            let k: &str = k.as_str();
            request.append_header(k, v.as_slice());
        }
        // set content-type from codec if none was provided
        if request.content_type().is_none() {
            if let Some(mime) = meta.codec.mime_types().first().copied() {
                if let Ok(mime) = Mime::from_str(mime) {
                    request.set_content_type(mime);
                }
            }
        }

        // Overwrite Host header
        // otherwise we might run into trouble when overwriting the endpoint.
        if let Some(host) = host {
            request.set_header("Host", host);
        }
        request.body_bytes(body);
        request
    }
}
*/
