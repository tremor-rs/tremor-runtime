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

use super::auth::Auth;
use crate::codec::{self, Codec};
use crate::connectors::impls::http::utils::{SurfRequest, SurfRequestBuilder, SurfResponse};
use crate::connectors::prelude::*;
use crate::connectors::utils::mime::MimeCodecMap;
use crate::postprocessor::{postprocess, Postprocessor, Postprocessors};
use crate::preprocessor::{preprocess, Preprocessor, Preprocessors};
use beef::Cow;
use halfbrown::HashMap;
use http_types::headers::HeaderValue;
use http_types::{Method, Mime};
use std::str::FromStr;
use tremor_common::time::nanotime;
use tremor_pipeline::{CbAction, Event, EventOriginUri, DEFAULT_STREAM_ID};
use tremor_script::{EventPayload, ValueAndMeta};
use tremor_value::{literal, structurize, Value};
use value_trait::{Builder, Mutable, ValueAccess};

// NOTE Initial attempt at extracting request/response handling for reuse in other HTTP based connectors
// TODO Extract headers into separate struct, separate out Config which is http client connector specific
// TODO Leverage default / static headers in surf?
// TODO Consider alternate httpc than surf due to issues with HTTP 1.0 backwards compatibility
// TODO Deterministic method of setting content-type
// TODO PBT's harness for full request/response client/server request/response lifecycle to test http conformance

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Target URL
    #[serde(default = "Default::default")]
    pub(crate) url: Url,
    /// Authorization method
    #[serde(default = "default_auth")]
    pub(crate) auth: Auth,
    /// Concurrency capacity limits ( in flight requests )
    #[serde(default = "default_concurrency")]
    pub(crate) concurrency: usize,
    /// Default HTTP headers
    #[serde(default = "Default::default")]
    pub(crate) headers: HashMap<String, Vec<String>>,
    /// Default HTTP method
    #[serde(default = "default_method")]
    pub(crate) method: String,
    /// MIME mapping to/from tremor codecs
    #[serde(default = "default_mime_codec_map")]
    pub(crate) codec_map: MimeCodecMap,
}

const DEFAULT_CONCURRENCY: usize = 4;

fn default_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}

fn default_method() -> String {
    "post".to_string()
}

fn default_auth() -> Auth {
    Auth::None
}

fn default_mime_codec_map() -> MimeCodecMap {
    MimeCodecMap::with_builtin()
}

// for new
impl ConfigImpl for Config {}
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

        // Calculate CB disposition using HTTP status code
        let _cb_disposition = if status.is_client_error() || status.is_server_error() {
            if log::log_enabled!(log::Level::Debug) {
                if let Ok(body) = response.body_string().await {
                    error!(
                        "[Rest::{}] HTTP request failed: {} => {}",
                        &origin_uri, status, body
                    );
                } else {
                    error!("[Rest:{}] HTTP request failed: {}", &origin_uri, status);
                }
            }
            CbAction::Fail
        } else {
            CbAction::Ack
        };

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

        // let codec = response
        //     .content_type()
        //     .and_then(|mime| codec_map.get_mut(mime.essence()))
        //     .map_or(self.codec, |c| -> &mut dyn Codec { c.as_mut() });

        //        let codec = self.codec.as_mut();

        let event = Self::body_to_event(
            codec,
            preprocessors,
            &origin_uri,
            rcvd_at,
            &meta,
            &mut response,
        )
        .await;

        Ok(event?)
    }
}
pub(crate) struct HttpRequestMeta {
    pub(crate) codec_map: MimeCodecMap,
    pub(crate) endpoint: Url,
    pub(crate) method: Method,
    //    pub(crate) auth: HttpAuth,
    pub(crate) headers: HashMap<String, Vec<HeaderValue>>,
    pub(crate) codec: Box<dyn Codec>,
    pub(crate) preprocessors: Vec<Box<dyn Preprocessor>>,
    pub(crate) postprocessors: Vec<Box<dyn Postprocessor>>,
}

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

type HeaderAndCodec<'outer> = (
    HashMap<String, Vec<HeaderValue>>,
    &'outer (dyn Codec + 'static),
);

impl HttpRequestMeta {
    fn refresh_active_codec<'event>(
        codec_map: &'event MimeCodecMap,
        name: &str,
        values: &[HeaderValue],
        active_codec: Option<&'event (dyn Codec + 'static)>,
    ) -> Option<&'event (dyn Codec + 'static)> {
        if "content-type".eq_ignore_ascii_case(name) {
            if let Some(active_codec) = active_codec {
                return Some(active_codec);
            } else if let Some(v) = values.first() {
                // NOTE - we do not currently handle attributes like `charset=UTF-8` correctly
                if let Ok(as_mime) = Mime::from_str(&v.to_string()) {
                    let essence = as_mime.essence();
                    return codec_map.map.get(essence).map(Box::as_ref);
                }
            }
        };

        // Fallthrough - use prior active
        active_codec
    }

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

    pub(crate) fn from_config(config: &ConnectorConfig, default_codec: &str) -> Result<Self> {
        let preprocessors = if let Some(preprocessors) = &config.preprocessors {
            crate::preprocessor::make_preprocessors(preprocessors)?
        } else {
            vec![]
        };
        let postprocessors = if let Some(postprocessors) = &config.postprocessors {
            crate::postprocessor::make_postprocessors(postprocessors)?
        } else {
            vec![]
        };

        let user_specified: Config = if let Some(user_specified) = &config.config {
            let config = user_specified.clone();
            structurize(config)? // Overrides - user specified
        } else {
            structurize(literal!({}))? // Defaults - see Config struct
        };

        let codec = if let Some(codec) = &config.codec {
            codec::resolve(codec)?
        } else {
            codec::resolve(&default_codec.into())?
        };

        let active_method = Method::from_str(&user_specified.method)?;

        let active_auth = user_specified.auth;

        let mut active_headers = HashMap::new();
        for (k, v) in user_specified.headers {
            // NOTE Not ideal as it doesn't capture conversion errors to HeaderValue
            // at the earliest opportunity
            let hv = v.into_iter().map(Value::from).collect();
            if let Some(_duplicate) = active_headers.insert(k, hv) {
                warn!("duplicate headers not allowed in config error");
            }
        }

        if let Some(auth_header) = active_auth.header_value()? {
            active_headers.insert("authorization".to_string(), Value::from(auth_header));
        }

        // Resolve user provided default HTTP headers
        let (active_headers, _active_codec) = Self::to_header_map(
            // TODO wire in
            &active_headers, // NOTE We box the config variant, as metadata overrides will always be boxed, whereas config occurs once
            &user_specified.codec_map,
            codec.as_ref(),
        )?;

        Ok(Self::new(
            user_specified.codec_map,
            user_specified.url,
            active_method,
            active_headers,
            codec,
            preprocessors,
            postprocessors,
        ))
    }

    fn new(
        codec_map: MimeCodecMap,
        default_endpoint: Url,
        default_method: Method,
        default_headers: HashMap<String, Vec<HeaderValue>>,
        codec: Box<dyn Codec>,
        preprocessors: Vec<Box<dyn Preprocessor>>,
        postprocessors: Vec<Box<dyn Postprocessor>>,
    ) -> Self {
        Self {
            codec_map,
            codec,
            preprocessors,
            postprocessors,
            endpoint: default_endpoint,
            headers: default_headers,
            method: default_method,
            //            auth: default_auth,
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

    pub(crate) fn process(&mut self, event: &Event) -> Result<(SurfRequest, Value<'static>)> {
        let mut body: Vec<u8> = vec![];
        let mut active_meta = BatchItemMeta {
            endpoint: self.endpoint.clone(),
            method: self.method,
            //            auth: self.auth.clone(),
            headers: self.headers.clone(),
            codec: self.codec.boxed_clone(),
            correlation: None,
        };
        for (_, chunk_meta) in event.value_meta_iter() {
            if let Some(_exists) = chunk_meta.get("request") {
                active_meta = self.per_batch_item_overrides(chunk_meta)?;
                break; // NOTE Multiple overrides in a batch make no sense - skip
            }
        }
        for (chunk_data, _) in event.value_meta_iter() {
            let encoded = active_meta.codec.encode(chunk_data)?;
            let processed = postprocess(&mut self.postprocessors, event.ingest_ns, encoded)?;
            for mut chunk in processed {
                body.append(&mut chunk);
            }
        }

        Ok((
            Self::into_request(&active_meta, &body)?,
            active_meta.to_value(),
        ))
    }

    fn into_request(meta: &BatchItemMeta, body: &[u8]) -> Result<SurfRequest> {
        debug!("Rest endpoint [{}] chosen", &meta.endpoint);
        let host = match (meta.endpoint.host(), meta.endpoint.port()) {
            (Some(host), Some(port)) => Some(format!("{}:{}", host, port)),
            (Some(host), _) => Some(host.to_string()),
            _ => None,
        };

        let mut request_builder = SurfRequestBuilder::new(meta.method, meta.endpoint.url().clone());

        // Build headers from meta - effectively overwrite config headers in case of conflict
        for (k, v) in &meta.headers {
            if "content-type".eq_ignore_ascii_case(k) {
                'inner: for hv in v {
                    if let Ok(mime) = Mime::from_str(hv.as_str()) {
                        request_builder = request_builder.content_type(mime);
                        break 'inner;
                    }
                }
            }
            let k: &str = k.as_str();
            request_builder = request_builder.header(k, v.as_slice());
        }

        // Overwrite Host header
        // otherwise we might run into trouble when overwriting the endpoint.
        if let Some(host) = host {
            request_builder = request_builder.header("Host", host);
        }
        request_builder = request_builder.body_bytes(body);
        Ok(request_builder.build())
    }
}
