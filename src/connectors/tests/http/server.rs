use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

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
    connectors::{
        impls::http::server,
        sink::SinkMsg,
        tests::{free_port, setup_for_tls, ConnectorHarness},
        utils::tls::{tls_client_config, TLSClientConfig},
    },
    errors::Result,
};
use async_std::prelude::FutureExt;
use http_client::{h1::H1Client, Body, Config as HttpClientConfig, HttpClient};
use http_types::{
    headers::{self, HeaderValue, HeaderValues},
    mime::BYTE_STREAM,
    Method, StatusCode, Url,
};
use std::str::FromStr;
use tremor_common::ports::IN;
use tremor_pipeline::{Event, EventId};
use tremor_script::ValueAndMeta;
use tremor_value::{literal, value::StaticValue, Value};
use value_trait::ValueAccess;

async fn handle_req<F>(
    req: surf::Request,
    handle_req_fn: F,
    connector: &ConnectorHarness,
    is_batch: bool,
) -> Result<surf::Response>
where
    F: Fn(&ValueAndMeta<'_>) -> ValueAndMeta<'static> + Send + 'static,
{
    let out = connector
        .out()
        .expect("No pipeline connected to out")
        .clone();
    let c_addr = connector.addr.clone();
    let handle = async_std::task::spawn::<_, Result<()>>(async move {
        while let Ok(inbound) = out.get_event().await {
            let response_data = handle_req_fn(inbound.data.suffix());
            let event = Event {
                id: inbound.id.clone(),
                data: response_data.into(),
                is_batch,
                ..Event::default()
            };
            c_addr.send_sink(SinkMsg::Event { event, port: IN }).await?;
        }
        Ok(())
    });
    let response = surf::client()
        .send(req)
        .timeout(Duration::from_secs(5))
        .await??;
    if let Some(res) = handle.cancel().await {
        res?;
    }
    Ok(response)
}

#[async_std::test]
async fn http_server_test() -> Result<()> {
    let _ = env_logger::try_init();
    let port = free_port::find_free_tcp_port().await?;
    let url = format!("http://localhost:{port}/");
    let defn = literal!({
        "codec": "json",
        "config": {
            "url": url.clone()
        }
    });
    let connector =
        ConnectorHarness::new(function_name!(), &server::Builder::default(), &defn).await?;
    connector.start().await?;
    connector.wait_for_connected().await?;

    // retry until the http server is actually up
    let start = Instant::now();
    let timeout = Duration::from_secs(30);

    // send an empty body, return request data in the body as json
    let req = surf::Request::builder(Method::Get, Url::parse(url.as_str())?)
        .body(Body::empty())
        .build();
    let mut res = handle_req(
        req.clone(),
        |req_data| {
            let value = literal!({
                "value": req_data.value().clone_static(),
                "meta": req_data.meta().get("http_server").get("request").map(tremor_script::Value::clone_static)
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
        &connector,
        false,
    )
    .await;

    while let Err(e) = res {
        if start.elapsed() > timeout {
            return Err(format!("HTTP Server not listening after {timeout:?}: {e}").into());
        }
        res = handle_req(
            req.clone(),
            |req_data| {
                let value = literal!({
                    "value": req_data.value().clone_static(),
                    "meta": req_data.meta().get("http_server").get("request").map(tremor_script::Value::clone_static)
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
            &connector,
            false,
        )
        .await;
    }
    let mut res = res?;

    assert_eq!(StatusCode::Created, res.status());
    let body = res.body_json::<StaticValue>().await?.into_value();
    assert_eq!(
        HeaderValue::from_str("application/json; charset=UTF-8")
            .ok()
            .as_ref(),
        res.header("content-type").map(HeaderValues::last)
    );
    assert_eq!(
        literal!({
            "meta": {
                "url_parts": {
                    "port": port,
                    "path": "/",
                    "host": "localhost",
                    "scheme": "http"
                },
                "url": url.clone(),
                "headers": {
                    "content-length": ["0"],
                    "content-type": ["application/octet-stream"], // h1-client injects this
                    "host": [format!("localhost:{port}")],
                    "connection": ["keep-alive"]
                },
                "method": "GET"
            },
            "value": null,
        }),
        body
    );

    // send patch request
    // with text/plain body
    // and return the request meta and body as a json (codec picked up from connector config)
    let req_url = format!("{}path/path/path?query=yes&another", url);
    let req = surf::Request::builder(Method::Patch, Url::parse(req_url.as_str())?)
        .header("content-type", "text/plain")
        .body_bytes("snot, badger".as_bytes())
        .build();
    let mut res = handle_req(
        req,
        |req_data| {
            let value = literal!({
                "value": req_data.value().clone_static(),
                "meta": req_data.meta().get("http_server").get("request").map(tremor_script::Value::clone_static)
            });
            let meta = literal!({
                "http_server": {
                    "response": {
                        "status": 400,
                        "headers": {
                            "some-other-header": ["foo", "bar"],
                        }
                    }
                }
            });
            (value, meta).into()
        },
        &connector,
        false,
    )
    .await?;
    assert_eq!(StatusCode::BadRequest, res.status());
    let body = res.body_json::<StaticValue>().await?.into_value();
    assert_eq!(
        HeaderValue::from_str("application/json").ok().as_ref(),
        res.header("content-type").map(HeaderValues::last)
    );
    assert_eq!(
        Some("[\"foo\", \"bar\"]".to_string()),
        res.header("some-other-header").map(ToString::to_string)
    );
    assert_eq!(
        literal!({
            "meta": {
                "url_parts": {
                    "port": port,
                    "query": "query=yes&another",
                    "path": "/path/path/path",
                    "host": "localhost",
                    "scheme": "http"
                },
                "url": req_url.clone(),
                "headers": {
                    "content-length": ["12"],
                    "content-type": ["text/plain"],
                    "host": [format!("localhost:{port}")],
                    "connection": ["keep-alive"]
                },
                "method": "PATCH"
            },
            "value": "snot, badger",
        }),
        body
    );

    // FIXME: test batched event with chunked encoding
    let req = surf::Request::builder(Method::Post, Url::parse(&url)?)
        .content_type(BYTE_STREAM)
        .body_string("A".repeat(1024))
        .build();
    let mut res = handle_req(
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
        &connector,
        true,
    )
    .await?;
    assert_eq!(Some(BYTE_STREAM), res.content_type());
    assert_eq!(StatusCode::Ok, res.status());
    assert_eq!(
        HeaderValue::from_str("chunked").ok().as_ref(),
        res.header(headers::TRANSFER_ENCODING)
            .map(HeaderValues::last)
    );
    let body = res.body_string().await?;
    assert_eq!("chunk_01|chunk_02|chunk_03|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", body.as_str());

    let (_out, err) = connector.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
async fn https_server_test() -> Result<()> {
    let _ = env_logger::try_init();
    setup_for_tls();
    let cert_file = "./tests/localhost.cert";
    let key_file = "./tests/localhost.key";

    let port = free_port::find_free_tcp_port().await?;
    let url = format!("https://localhost:{port}/");
    let defn = literal!({
        "codec": "json",
        "config": {
            "url": url.clone(),
            "tls": {
                "cert": cert_file,
                "key": key_file
            }
        }
    });
    let connector =
        ConnectorHarness::new(function_name!(), &server::Builder::default(), &defn).await?;
    connector.start().await?;
    connector.wait_for_connected().await?;
    let out = connector
        .out()
        .expect("No pipeline connected to out")
        .clone();
    let c_addr = connector.addr.clone();

    // respond to requests with value and meta in body, encoded as yaml
    let handle = async_std::task::spawn::<_, Result<()>>(async move {
        while let Ok(inbound) = out.get_event().await {
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
            c_addr.send_sink(SinkMsg::Event { event, port: IN }).await?;
        }
        Ok(())
    });
    let mut config = HttpClientConfig::new();

    let tls_config = tls_client_config(&TLSClientConfig {
        cafile: Some(PathBuf::from_str(cert_file).map_err(|_| "bad cert")?),
        domain: Some("localhost".to_string()),
        cert: None,
        key: None,
    })
    .await?;
    config = config.set_tls_config(Some(Arc::new(tls_config)));
    config = config.set_timeout(Some(Duration::from_secs(20)));
    let client = H1Client::try_from(config).map_err(|_| "bad config")?;

    let req = http_types::Request::new(Method::Delete, Url::parse(&url)?);
    let mut response = client.send(req.clone()).await;

    let start = Instant::now();
    let timeout = Duration::from_secs(30);
    while let Err(e) = response {
        async_std::task::sleep(Duration::from_millis(100)).await;
        if start.elapsed() > timeout {
            return Err(format!("Timeout waiting for HTTPS server to boot up: {e}").into());
        }
        response = client.send(req.clone()).await;
    }
    let mut response = response?;
    let body = response.body_string().await?;
    assert_eq!(
        format!(
            r#"meta:
  url_parts:
    port: {port}
    scheme: https
    host: localhost
    path: /
  url: https://localhost:{port}/
  method: DELETE
  headers:
    content-length:
    - '0'
    content-type:
    - application/octet-stream
    host:
    - localhost:{port}
    connection:
    - keep-alive
value: null
"#
        ),
        body
    );
    assert_eq!(StatusCode::Ok, response.status());
    assert_eq!(
        HeaderValue::from_str("application/yaml; charset=UTF-8")
            .ok()
            .as_ref(),
        response.header("content-type").map(HeaderValues::last)
    );
    if let Some(res) = handle.cancel().await {
        res?;
    }

    Ok(())
}
