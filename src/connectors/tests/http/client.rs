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
        impls::http,
        prelude::Url,
        tests::{free_port::find_free_tcp_port, setup_for_tls, ConnectorHarness},
        utils::url::HttpDefaults,
    },
    errors::Result,
};
use async_std::{
    io::Cursor,
    task::{spawn, JoinHandle},
};
use http_types::{
    headers::{HeaderValues, CONTENT_TYPE, TRANSFER_ENCODING},
    Body,
};
use rustls::NoClientAuth;
use tide;
use tide_rustls::TlsListener;
use tremor_common::ports::IN;
use tremor_pipeline::Event;
use tremor_script::{literal, Value, ValueAndMeta};
use value_trait::{Mutable, ValueAccess};

/// Find free TCP host:port for use in test server endpoints
pub(crate) async fn find_free_tcp_endpoint_str() -> String {
    let port = find_free_tcp_port().await.unwrap_or(65535);
    format!("localhost:{port}") // NOTE we use localhost rather than an IP for cmopat with TLS
}

struct TestHttpServer {
    acceptor: Option<JoinHandle<Result<()>>>,
}

async fn fake_server_dispatch(mut reqest: tide::Request<()>) -> tide::Result<tide::Response> {
    use tide::StatusCode;
    let mut res = tide::Response::new(StatusCode::Ok);
    dbg!(&reqest);
    let chunked = reqest
        .header(TRANSFER_ENCODING)
        .map(HeaderValues::last)
        .filter(|hv| hv.as_str() == "chunked")
        .is_some();

    let body = reqest.body_bytes().await?;

    if chunked {
        res.set_content_type(http_types::mime::PLAIN);
        res.set_body(Body::from_reader(Cursor::new(body), None));
    } else {
        if let Some(ct) = reqest.content_type() {
            res.set_content_type(ct);
        }
        res.set_body(body);
    }

    Ok(res)
}

impl TestHttpServer {
    async fn new(raw_url: String) -> Result<Self> {
        let mut instance = TestHttpServer { acceptor: None };
        instance.acceptor = Some(spawn(async move {
            let url: Url<HttpDefaults> = Url::parse(&raw_url)?;
            if "https" == url.scheme() {
                let cert_file = "./tests/localhost.cert";
                let key_file = "./tests/localhost.key";
                setup_for_tls(); // Setups up TLS certs for localhost testing as a side-effect

                let mut endpoint = tide::Server::new();
                endpoint.at("/").all(fake_server_dispatch);
                endpoint.at("/*").all(fake_server_dispatch);
                if let Err(e) = endpoint
                    .listen(
                        TlsListener::build()
                            .config(rustls::ServerConfig::new(NoClientAuth::new()))
                            .addrs(url.url().socket_addrs(|| None)?[0])
                            .cert(cert_file)
                            .key(key_file),
                    )
                    .await
                {
                    error!("Error listening on {url}: {e}");
                }
            } else {
                let mut endpoint = tide::Server::new();
                endpoint.at("/").all(fake_server_dispatch);
                endpoint.at("/*").all(fake_server_dispatch);
                if let Err(e) = endpoint.listen(url.url().clone()).await {
                    error!("Error listening on {url}: {e}");
                }
            };
            Ok(())
        }));
        Ok(instance)
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(acceptor) = self.acceptor.take() {
            acceptor.cancel().await;
        }
        Ok(())
    }
}

// Convenience template for a round trip HTTP request/response interaction
async fn rtt(
    scheme: &'static str,
    target: &str,
    codec: &'static str,
    auth: Option<Value<'static>>,
    event: Event,
) -> Result<ValueAndMeta<'static>> {
    let _ = env_logger::try_init();
    let url = format!("{scheme}://{target}");
    let mut config = literal!({
        "url": url.clone(),
        "method": "get",
        "mime_mapping": {
            "application/json": "json",
            "application/yaml": "yaml",
            "*/*": codec,
        },
    });
    if let Some(auth) = auth {
        config.try_insert("auth", auth.clone_static());
    }
    let defn = literal!({
      "config": config,
    });

    let mut fake = TestHttpServer::new(url.clone()).await?;

    let harness =
        ConnectorHarness::new(function_name!(), &http::client::Builder::default(), &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of connector");

    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    harness.send_to_sink(event, IN).await?;

    let event = out_pipeline.get_event().await?;
    fake.stop().await?;
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    let (value, meta) = event.data.parts();
    let vm: ValueAndMeta<'static> =
        ValueAndMeta::from_parts(value.clone_static(), meta.clone_static());
    Ok(vm)
}

macro_rules! assert_with_request_meta {
    ($res: expr, $meta: ident, $ctx: block) => {
        let rqm = $res.meta().get("http_client");
        let rqm = rqm.get("request");
        if let Some($meta) = rqm {
            $ctx
        } else {
            assert!(false, "Expected request metadata to be set by connector",);
        }
    };
}

macro_rules! assert_with_response_headers {
    ($res: expr, $meta: ident, $ctx: block) => {
        let rqm = $res.meta().get("http_client");
        let rqm = rqm.get("response");
        if let Some($meta) = rqm {
            let rqm = $meta.get("headers");
            if let Some($meta) = rqm {
                $ctx
            } else {
                assert!(
                    false,
                    "Expected response headers metadata to be set by connector",
                );
            }
        } else {
            assert!(false, "Expected response metadata to be set by connector",);
        }
    };
}

macro_rules! assert_with_request_headers {
    ($res: expr, $meta: ident, $ctx: block) => {
        let rqm = $res.meta().get("http_client");
        let rqm = rqm.get("request");
        if let Some($meta) = rqm {
            let rqm = $meta.get("headers");
            if let Some($meta) = rqm {
                $ctx
            } else {
                assert!(
                    false,
                    "Expected response headers metadata to be set by connector",
                );
            }
        } else {
            assert!(false, "Expected response metadata to be set by connector",);
        }
    };
}

#[async_std::test]
async fn http_client_request_with_defaults() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let event = Event {
        data: (
            literal!(null),
            literal!({
                "http_client": {
                    "request": {},
                },
                "correlation": "snot"
            }),
        )
            .into(),
        transactional: true,
        ..Default::default()
    };
    let res = rtt("http", &target, "string", None, event).await?;
    assert_eq!(&Value::from("null"), res.value());
    Ok(())
}

#[async_std::test]
async fn http_client_request_override_method() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let event = Event {
        data: (
            Value::from(""),
            literal!({
                "http_client": {
                    "request": {
                        "method": "get",
                    }
                }
            }),
        )
            .into(),
        ..Default::default()
    };
    let res = rtt("http", &target, "string", None, event).await?;
    // empty response body
    assert_eq!(&Value::from(""), res.value());
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some("GET"), meta.get_str("method"));
    });
    Ok(())
}

#[async_std::test]
async fn http_client_request_override_endpoint() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let event = Event {
        data: (
            Value::const_null(),
            literal!({
                "http_client": {
                    "request": {
                        "method": "put",
                        "url": format!("http://{}/snot/badger?flork=mork", target)
                    }
                }
            }),
        )
            .into(),
        ..Default::default()
    };
    let res = rtt("http", &target, "string", None, event).await?;

    let overriden_url: &str = &format!("http://{}/snot/badger?flork=mork", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(overriden_url), meta.get_str("url"));
        assert_eq!(Some("PUT"), meta.get_str("method"));
    });

    assert_eq!(&Value::from("null"), res.value());
    Ok(())
}

#[async_std::test]
async fn http_client_request_override_codec() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let event = Event {
        data: (
            literal!({ "snot": "badger" }),
            literal!({
                "http_client": {
                    "request": {
                        "method": "patch",
                        "headers": {
                            "content-type": "application/yaml"
                        }
                    }
                }
            }),
        )
            .into(),
        ..Default::default()
    };
    let res = rtt("http", &target, "json", None, event).await?;

    let base_url: &str = &format!("http://{}/", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("url"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
    });

    assert_eq!(
        literal!({
            "snot": "badger"
        }),
        res.value()
    );
    Ok(())
}

#[async_std::test]
async fn http_client_request_override_headers() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let event = Event {
        data: (
            Value::from(42),
            literal!({
                "http_client": {
                    "request": {
                        "method": "patch",
                        "headers": { "x-snot": [ "badger", "badger", "badger"] }
                    }
                },
                "correlation": "snot"
            }),
        )
            .into(),
        ..Default::default()
    };
    let res = rtt("http", &target, "string", None, event).await?;

    let base_url: &str = &format!("http://{}/", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("url"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
    });
    assert_eq!(Some(&Value::from("snot")), res.meta().get("correlation"));

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["badger", "badger", "badger"])),
            meta.get("x-snot")
        );
    });

    assert_with_response_headers!(res, meta, {
        assert_eq!(Some(&literal!(["2"])), meta.get("content-length"));
    });

    assert_eq!(Value::from("42"), res.value());
    Ok(())
}

#[async_std::test]
async fn http_client_request_override_content_type() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let event = Event {
        data: (
            literal!([{"snot": "badger"}, 42.0]),
            literal!({
                "http_client": {
                    "request": {
                        "method": "patch",
                        "headers": { "content-type": [ "application/json"] }
                    }
                },
                "correlation": "snot"
            }),
        )
            .into(),
        transactional: true,
        ..Default::default()
    };
    let res = rtt("http", &target, "string", None, event).await?;

    let base_url: &str = &format!("http://{}/", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("url"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
    });

    assert_eq!(Some(&Value::from("snot")), res.meta().get("correlation"));

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["application/json"])),
            meta.get("content-type")
        );
    });

    assert_with_response_headers!(res, meta, {
        assert_eq!(Some(&literal!(["24"])), meta.get("content-length"));
        assert_eq!(
            Some(&literal!(["application/json"])),
            meta.get("content-type")
        );
    });

    assert_eq!(
        literal!([
            {"snot": "badger"},
            42.0
        ]),
        res.value()
    );
    Ok(())
}

#[async_std::test]
async fn http_client_request_auth_none() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let event = Event {
        data: (
            literal!({"snot": "badger"}),
            literal!({
                "http_client": {
                    "request": {
                        "method": "patch",
                        "headers": { "Content-type": [ "application/json"] }
                    }
                },
                "correlation": "snot"
            }),
        )
            .into(),
        transactional: true,
        ..Default::default()
    };
    let res = rtt("http", &target, "string", Some(literal!("none")), event).await?;

    let base_url: &str = &format!("http://{}/", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("url"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
    });

    assert_eq!(Some(&Value::from("snot")), res.meta().get("correlation"));

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["application/json"])), // NOTE - connector does not respect this and uses codec instead - see below for alternate
            meta.get(CONTENT_TYPE.as_str())
        );
    });

    assert_with_response_headers!(res, meta, {
        assert_eq!(Some(&literal!(["17"])), meta.get("content-length"));
        // the server mirrors the request content-type
        assert_eq!(
            Some(&literal!(["application/json"])),
            meta.get(CONTENT_TYPE.as_str())
        );
    });

    assert_eq!(
        literal!({
            "snot": "badger"
        }),
        res.value()
    );
    Ok(())
}

#[async_std::test]
async fn http_client_request_auth_basic() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let event = Event {
        data: (
            literal!({"snot": "badger"}),
            literal!({
                "http_client": {
                    "request": {
                        "method": "PATCh",
                        "headers": { "Content-TYPE": [ "application/json"] }
                    }
                },
                "correlation": "snot"
            }),
        )
            .into(),
        transactional: true,
        ..Default::default()
    };
    let res = rtt(
        "http",
        &target,
        "string",
        Some(literal!({
            "basic": {
                "username": "snot",
                "password": "badger"
            }
        })),
        event,
    )
    .await?;

    let base_url: &str = &format!("http://{}/", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("url"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
    });
    assert_eq!(Some(&Value::from("snot")), res.meta().get("correlation"));

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["application/json"])),
            meta.get("content-type")
        );
        assert_eq!(
            Some(&literal!(["Basic c25vdDpiYWRnZXI="])),
            meta.get("authorization")
        );
    });

    assert_with_response_headers!(res, meta, {
        assert_eq!(Some(&literal!(["17"])), meta.get("content-length"));
        assert_eq!(
            Some(&literal!(["application/json"])),
            meta.get("content-type")
        );
    });

    assert_eq!(literal!({"snot": "badger"}), res.value());
    Ok(())
}

#[async_std::test]
async fn chunked() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let event = Event {
        data: (
            literal!([
                {
                    "data": {
                        "value": "chunk01 ",
                        "meta": {
                            "http_client": {
                                "request": {
                                    "method": "PATCh",
                                    "headers": {
                                        "content-TYPE": [ "application/json"],
                                        "transfer-Encoding": "chunked"
                                    }
                                }
                            },
                            "correlation": "badger"
                        }
                    },
                },
                {
                    "data": {
                        "value": "chunk02 ",
                        "meta": {
                            "http_client": {
                                "request": {
                                    "method": "ignored",
                                    "headers": {
                                        "ignored": "true"
                                    }
                                }
                            }
                        }
                    }
                }
            ]),
            literal!({}),
        )
            .into(),
        is_batch: true,
        ..Default::default()
    };
    let res = rtt("http", &target, "string", None, event).await?;
    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["chunked"])),
            meta.get(TRANSFER_ENCODING.as_str())
        );
        assert_eq!(
            Some(&literal!(["application/json"])),
            meta.get("content-type")
        );
    });
    // the fake server is setup to answer chunked requests with chunked responses
    assert_with_response_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["chunked"])),
            meta.get(TRANSFER_ENCODING.as_str())
        );
        assert_eq!(
            // the fake server sends text/plain
            Some(&literal!(["text/plain;charset=utf-8"])),
            meta.get("content-type")
        );
    });
    // the quotes are artifacts from request json encoding
    assert_eq!(&Value::from("\"chunk01 \"\"chunk02 \""), res.value());
    Ok(())
}

#[async_std::test]
async fn missing_tls_config_https() -> Result<()> {
    let defn = literal!({
      "config": {
        "url": "https://localhost:12345"
      },
      "codec": "influx",
    });
    let id = function_name!();
    let res = ConnectorHarness::new(id, &http::client::Builder::default(), &defn)
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();

    assert_eq!("Invalid Definition for connector \"test::missing_tls_config_https\": missing tls config with 'https' url. Set 'tls' to 'true' or provide a full tls config.", res);

    Ok(())
}

#[async_std::test]
async fn missing_config() -> Result<()> {
    let defn = literal!({
      "codec": "binflux",
    });
    let id = function_name!();
    let res = ConnectorHarness::new(id, &http::client::Builder::default(), &defn)
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();

    assert!(res.contains("Missing Configuration"));

    Ok(())
}
