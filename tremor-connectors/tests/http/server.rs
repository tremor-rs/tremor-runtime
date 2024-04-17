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
use anyhow::{anyhow, Result};
use http::StatusCode;
use hyper::body::HttpBody;
use hyper::{body::to_bytes, Body, Request, Response};
use hyper_rustls::HttpsConnectorBuilder;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::time::timeout;
use tremor_connectors::{
    harness::Harness,
    impls::http::{meta::content_type, server},
};
use tremor_connectors_test_helpers::{free_port, setup_for_tls};
use tremor_script::ValueAndMeta;
use tremor_system::event::{Event, EventId};
use tremor_value::{literal, Value};
use value_trait::prelude::*;

/// This function takes a harness and connects it the following way:
///
/// `req` -> `source` -> `handle_req_fn` -> `sink` -> `response`
///
/// in the meantime it creates a new hyper client and sends the provided `req` to the source
/// then returns the response after it went through `source`, `handle_req_fn` and `sink`
async fn handle_req<F>(
    req: Request<Body>,
    handle_req_fn: F,
    mut connector: Harness,
    is_batch: bool,
) -> Result<Response<Body>>
where
    F: Fn(&ValueAndMeta<'_>) -> ValueAndMeta<'static> + Send + 'static,
{
    let handle = tokio::task::spawn(async move {
        // listen to all events comming from the source
        while let Ok(inbound) = connector.out()?.get_event().await {
            // on a event from the source, process it with the handle_req_fn
            let response_data = handle_req_fn(inbound.data.suffix());
            // create a response event
            let event = Event {
                id: inbound.id.clone(),
                data: response_data.into(),
                is_batch,
                ..Event::default()
            };
            // pass the processed event to the sink
            connector.send_to_sink(event).await?;
        }
        let (_out, _err) = connector.stop().await?;

        anyhow::Ok(())
    });
    let response = timeout(
        Duration::from_secs(5),
        hyper::client::Client::new().request(req),
    )
    .await??;
    handle.abort();
    Ok(response)
}

async fn harness(scheme: &str, mut config: Value<'static>) -> Result<(Harness, String, u16)> {
    let port = free_port::find_free_tcp_port().await?;
    let url = format!("{scheme}://localhost:{port}/");
    config.try_insert("url", url.clone());
    let defn = literal!({ "config": config });
    let connector = Harness::new("test", &server::Builder::default(), &defn).await?;
    connector.start().await?;
    connector.wait_for_connected().await?;
    Ok((connector, url, port))
}

async fn harness_dflt(scheme: &str) -> Result<(Harness, String, u16)> {
    harness(scheme, literal!({})).await
}

#[tokio::test(flavor = "multi_thread")]
async fn http_server_test_basic() -> Result<()> {
    // retry until the http server is actually up
    let start = Instant::now();
    let timeout = Duration::from_secs(30);

    // send an empty body, return request data in the body as json

    let mut result = Err(anyhow!("todo"));

    let mut final_port = 0;
    while result.is_err() {
        if start.elapsed() > timeout {
            return Err(anyhow!(
                "HTTP Server not listening after {timeout:?}: {result:?}"
            ));
        }
        let Ok((connector, url, port)) = harness_dflt("http").await else {
            continue;
        };
        final_port = port;
        let req = Request::builder()
            .method("GET")
            .uri(url.as_str())
            .body(Body::empty())?;
        result = handle_req(
            req,
            |req_data| {
                let value = literal!({
                    "value": req_data.value().clone_static(),
                    "meta": req_data.meta().get("http_server").get("request").map(tremor_value::Value::clone_static)
                });
                let meta = literal!({
                    "http_server": {
                        "response": {
                            "status": 201,
                            "headers": {
                                "content-type": "application/json; charset=UTF-8"
                            }
                        }
                    }
                });
                (value, meta).into()
            },
            connector,
            false,
        )
        .await;
    }
    let result = result?;

    assert_eq!(StatusCode::CREATED, result.status());
    let h = result
        .headers()
        .get("content-type")
        .map(|a| a.as_bytes().to_vec());
    let mut body = to_bytes(result.into_body()).await?.to_vec();
    let body = simd_json::from_slice::<Value>(&mut body)?.into_static();
    assert_eq!(Some(b"application/json; charset=UTF-8".to_vec()), h);
    assert_eq!(
        literal!({
            "meta": {
                "headers": {
                    "host": [format!("localhost:{final_port}")],
                },
                "method": "GET",
                "protocol": "http",
                "uri": "/",
                "version": "HTTP/1.1",
            },
            "value": null,
        }),
        body
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_server_test_query() -> Result<()> {
    // send patch request
    // with text/plain body
    // and return the request meta and body as a json (codec picked up from connector config)
    let (connector, url, port) = harness_dflt("http").await?;
    let req_url = format!("{url}path/path/path?query=yes&another");
    let req = Request::builder()
        .method("PATCH")
        .uri(req_url.clone())
        .header("content-type", "text/plain")
        .body(Body::from("snot, badger"))?;

    let  result = handle_req(
        req,
        |req_data| {
            let value = literal!({
                "value": req_data.value().clone_static(),
                "meta": req_data.meta().get("http_server").get("request").map(tremor_value::Value::clone_static)
            });
            let meta = literal!({
                "http_server": {
                    "response": {
                        "status": 400,
                        "headers": {
                            "some-other-header": ["foo", "bar"],
                            "content-type": "application/json"
                        }
                    }
                }
            });
            (value, meta).into()
        },
        connector,
        false,
    )
    .await?;
    assert_eq!(StatusCode::BAD_REQUEST, result.status());
    let h1 = result
        .headers()
        .get("content-type")
        .map(|h| String::from_utf8_lossy(h.as_bytes()).to_string());
    let h2: Vec<String> = result
        .headers()
        .get_all("some-other-header")
        .iter()
        .map(|h| String::from_utf8_lossy(h.as_bytes()).to_string())
        .collect();
    let mut body = to_bytes(result.into_body()).await?.to_vec();
    let body = simd_json::from_slice::<Value>(&mut body)?.into_static();
    assert_eq!(Some("application/json".to_string()), h1);
    assert_eq!(vec!["foo".to_string(), "bar".to_string()], h2);
    assert_eq!(
        literal!({
            "value": "snot, badger",
            "meta": {
                "protocol": "http",
                "uri": "/path/path/path?query=yes&another",
                "headers": {
                    "content-length": ["12"],
                    "content-type": ["text/plain"],
                    "host": [format!("localhost:{port}")],
                },
                "method": "PATCH",
                "version": "HTTP/1.1",
            },
        }),
        body
    );
    Ok(())
}
#[tokio::test(flavor = "multi_thread")]
async fn http_server_test_chunked() -> Result<()> {
    // TODO: test batched event with chunked encoding
    let (connector, url, _port) = harness_dflt("http").await?;
    let req = Request::builder()
        .method("POST")
        .uri(url)
        .header(
            hyper::header::CONTENT_TYPE,
            mime::APPLICATION_OCTET_STREAM.to_string(),
        )
        .body(Body::from("A".repeat(1024)))?;

    let result = handle_req(
        req,
        |req_data| {
            let value = literal!([{
                "data": {
                    "value": "chunk_01|",
                    "meta": {
                        "http_server": {
                            "response": {
                                "status": 200,
                                "headers": {
                                    "transfer-encoding": "chunked",
                                    "content-type": "application/octet-stream"
                                }
                            }
                        }
                    }
                }
            },
            {
                "data": {
                    "value": "chunk_02|",
                    "meta": {
                        "second_meta": "should be ignored",
                        "http_server": {
                            "response": {
                                "status": 503,
                                "headers": {
                                    "content-type": "application/json"
                                }
                            }
                        }
                    }
                }
            },
            {
                "data": {
                    "value": "chunk_03|",
                    "meta": {}
                }
            },
            {
                "data": {
                    "value": req_data.value().clone_static(),
                    "meta": {}
                }
            }]);
            let meta = literal!({});
            (value, meta).into()
        },
        connector,
        true,
    )
    .await?;
    assert_eq!(
        Some(mime::APPLICATION_OCTET_STREAM),
        content_type(Some(result.headers()))?
    );
    assert_eq!(StatusCode::OK, result.status());
    assert_eq!(
        Some(&b"chunked"[..]),
        result
            .headers()
            .get(hyper::header::TRANSFER_ENCODING)
            .map(http::HeaderValue::as_bytes)
    );
    let body = to_bytes(result.into_body()).await?.to_vec();
    assert_eq!(b"chunk_01|chunk_02|chunk_03|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", body.as_slice());

    Ok(())
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread")]
async fn https_server_test() -> Result<()> {
    setup_for_tls();
    let cert_file = "./tests/localhost.cert";
    let key_file = "./tests/localhost.key";

    let port = free_port::find_free_tcp_port().await?;
    let url = format!("https://localhost:{port}/");
    let defn = literal!({
        "config": {
            "url": url.clone(),
            "tls": {
                "cert": cert_file,
                "key": key_file
            },
        }
    });
    let mut connector = Harness::new("test", &server::Builder::default(), &defn).await?;
    connector.start().await?;
    connector.wait_for_connected().await?;
    // let c_addr = connector.addr.clone();

    // respond to requests with value and meta in body, encoded as yaml
    let handle = tokio::task::spawn(async move {
        while let Ok(inbound) = connector.out()?.get_event().await {
            let inbound_value = inbound.data.suffix().value();
            let inbound_meta = inbound.data.suffix().meta();
            let value = literal!({
                "meta":  inbound_meta.get("http_server").get("request").map(Value::clone_static),
                "value": inbound_value.clone_static()
            });
            let meta = literal!({
                "http_server": {
                    // set request_id to resolve the request_id from metadata instead of event id
                    "request_id":  inbound_meta.get("http_server").get("request_id").map(Value::clone_static),
                    "response": {
                        "headers": {
                            "content-type": "application/yaml; charset=UTF-8",
                        }
                    }
                }
            });
            let event = Event {
                id: EventId::default(),
                data: (value, meta).into(),
                ..Event::default()
            };
            connector.send_to_sink(event).await?;
        }
        anyhow::Ok(())
    });

    let tls_config = tremor_connectors::utils::tls::TLSClientConfig::new(
        Some(PathBuf::from(cert_file)),
        Some("localhost".to_string()),
        None,
        None,
    )
    .to_client_config()?;
    let transport = HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();

    // HttpsConnector::from((HttpConnector::new(), Arc::new(tls_config))).;

    let client = hyper::Client::builder().build(transport);

    let req = hyper::Request::builder()
        .method("DELETE")
        .uri(&url)
        .body(hyper::Body::empty())?;
    let one_sec = Duration::from_secs(1);
    let mut response = timeout(one_sec, client.request(req)).await;

    let start = Instant::now();
    let max_timeout = Duration::from_secs(30);
    while let Err(e) = response {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let req = hyper::Request::builder()
            .method("DELETE")
            .uri(&url)
            .body(hyper::Body::empty())?;
        if start.elapsed() > max_timeout {
            return Err(anyhow!("Timeout waiting for HTTPS server to boot up: {e}"));
        }
        response = timeout(one_sec, client.request(req)).await;
    }
    let mut response = response??;
    let mut data: Vec<u8> = Vec::new();
    while let Some(chunk) = response.data().await.transpose()? {
        data.extend_from_slice(&chunk);
    }
    let body = String::from_utf8(data)?;
    let body = serde_yaml::from_str::<serde_yaml::Value>(&body)?;
    let expected = serde_yaml::from_str::<serde_yaml::Value>(&format!(
        r#"
meta:
  uri: /
  version: HTTP/1.1
  protocol: http
  headers:
    host:
    - localhost:{port}
  method: DELETE
value: null
  "#
    ))?;
    assert_eq!(expected, body);
    assert_eq!(StatusCode::OK, response.status());
    assert_eq!(
        Some("application/yaml; charset=UTF-8"),
        response
            .headers()
            .get("content-type")
            .and_then(|c| c.to_str().ok())
    );
    handle.abort();
    Ok(())
}
