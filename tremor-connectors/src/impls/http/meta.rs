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

use crate::utils::mime::MimeCodecMap;
use http::{header::ToStrError, HeaderMap};
use hyper::{header::HeaderValue, Request, Response};
use mime::Mime;
use tremor_config::NameWithConfig;
use tremor_value::prelude::*;

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
/// # Errors
/// - if the request metadata can not be extracted
#[allow(clippy::module_name_repetitions)] // TODO refactor rename
pub fn extract_request_meta<T>(
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
