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

use anyhow::{Error, Result};
use azure_core::headers::Headers;
use azure_core::Request;
use azure_core::Response;
use beef::Cow;
use bytes::Bytes;
use simd_json::ObjectHasher;
use tremor_value::literal;
use tremor_value::{StaticNode, Value};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RequestId(u64);

impl RequestId {
    pub(crate) fn new(id: u64) -> Self {
        Self(id)
    }
    pub(crate) fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub(crate) struct RequestMeta {
    method: String,
    url: String,
    headers: Headers,
}

impl RequestMeta {
    pub fn default() -> Self {
        Self {
            method: String::new(),
            url: String::new(),
            headers: Headers::new(),
        }
    }
}

impl From<RequestMeta> for Value<'static> {
    fn from(meta: RequestMeta) -> Value<'static> {
        let mut headers =
            tremor_value::Object::with_capacity_and_hasher(5, ObjectHasher::default());
        for (key, value) in meta.headers.iter() {
            headers.insert(
                Cow::owned(key.as_str().to_string()),
                Value::String(Cow::owned(value.as_str().to_string())),
            );
        }

        literal!(
            {
                "method": meta.method,
                "url": meta.url,
                "headers": Value::Object(Box::new(headers)),
            }
        )
    }
}

pub(crate) struct ResponseMeta {
    status: u16,
    headers: Headers,
    pub(crate) data: Value<'static>,
    pub(crate) content_length: usize,
}

impl From<ResponseMeta> for Value<'static> {
    fn from(meta: ResponseMeta) -> Value<'static> {
        let mut headers =
            tremor_value::Object::with_capacity_and_hasher(5, ObjectHasher::default());
        for (key, value) in meta.headers.iter() {
            headers.insert(
                Cow::owned(key.as_str().to_string()),
                Value::String(Cow::owned(value.as_str().to_string())),
            );
        }

        literal!(
            {
                "status": meta.status,
                "headers": Value::Object(Box::new(headers)),
                "content_length": meta.content_length,
                "data": meta.data
            }
        )
    }
}

impl ResponseMeta {
    pub fn default() -> Self {
        Self {
            status: 0,
            headers: Headers::new(),
            data: Value::Static(StaticNode::Null),
            content_length: 0,
        }
    }
}

pub(crate) fn extract_request_meta(request: &Request) -> RequestMeta {
    let mut meta = RequestMeta::default();
    meta.method = request.method().to_string();
    meta.url = request.url().to_string();
    meta.headers = request.headers().clone();
    meta
}

pub(crate) async fn extract_response_meta(response: Response) -> Result<(ResponseMeta, Vec<u8>), Error> {
    let (status, headers, body) = response.deconstruct();
    let data: Bytes = body.collect().await?;
    let mut data = data.to_vec();
    let len = data.len();

    let mut meta = ResponseMeta::default();
    meta.status = status as u16;
    meta.headers = headers.clone();
    meta.content_length = len;

    Ok((meta, data))
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use super::*;
    use azure_core::Method;
    use azure_core::Request;
    use azure_core::StatusCode;
    use futures::Stream;
    use url::Url;

    #[test]
    fn request_id_to_string() {
        let request_id = RequestId::new(123);
        assert_eq!(request_id.to_string(), "123");
    }

    // Custom type that wraps a pinned Stream<Bytes> for testing azure::Response
    struct MyPinnedStream {
        inner: Pin<Box<(dyn Stream<Item = Result<Bytes, azure_core::Error>> + Send + Sync)>>,
    }

    unsafe impl Send for MyPinnedStream {}
    unsafe impl Sync for MyPinnedStream {}

    fn create_pinned_stream(fake_data: &'static str) -> MyPinnedStream {
        let stream = futures::stream::once(async move { Ok(Bytes::from(fake_data)) });
        MyPinnedStream {
            inner: Box::pin(stream),
        }
    }

    #[test]
    fn test_request_meta() -> anyhow::Result<()> {
        let url = Url::parse("http://example.com")?;
        let mut request = Request::new(url, Method::Get);
        request.insert_header("content-type", "application/json");
        request.insert_header("authorization", "Bearer token");

        let meta = extract_request_meta(&request);
        let expected = literal!({
            "method": "GET",
            "url": "http://example.com/",
            "headers": {
                "content-type": "application/json",
                "authorization": "Bearer token"
            }
        });

        assert_eq!(Value::from(meta), expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_response_meta() -> anyhow::Result<()> {
        let mut headers = azure_core::headers::Headers::new();
        headers.insert("content-type", "application/json");
        headers.insert("content-length", "10");
        let data = r#""snot""#;
        let stream = create_pinned_stream(data);
        let inner = stream.inner;
        let response = Response::new(StatusCode::Ok, headers, inner);

        let meta = extract_response_meta(response).await?;
        let expected = literal!({
            "status": 200,
            "headers": {
                "content-type": "application/json",
                "content-length": "10"
            },
            "content_length": 6,
            "data": "snot"
        });

        assert_eq!(Value::from(meta), expected);

        Ok(())
    }
}
