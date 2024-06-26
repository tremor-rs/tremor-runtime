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

use anyhow::Result;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::StatusCode;
use hyper::{service::service_fn, Response};
use hyper_util::rt::TokioIo;
use log::error;
use rand::{seq::SliceRandom, thread_rng};
use std::net::ToSocketAddrs;
use tokio::net::TcpListener;
use tokio::task::{spawn, JoinHandle};
use tremor_common::url::HttpDefaults;
use tremor_common::url::Url;
use tremor_connectors::{
    harness::Harness,
    impls::http::{self as http_impl, meta::content_type},
};
use tremor_connectors_test_helpers::free_port::find_free_tcp_port;
use tremor_script::ValueAndMeta;
use tremor_system::{
    controlplane::CbAction,
    event::{Event, EventId, DEFAULT_PULL_ID},
};
use tremor_value::{literal, Value};
use value_trait::prelude::*;

/// Find free TCP host:port for use in test server endpoints
pub(crate) async fn find_free_tcp_endpoint_str() -> Result<String> {
    let port = find_free_tcp_port().await?;
    Ok(format!("localhost:{port}")) // NOTE we use localhost rather than an IP for cmopat with TLS
}

fn host_to_sockaddr(hostport: &str) -> Result<std::net::SocketAddr, String> {
    let mut rng = thread_rng();
    let socket_addrs: Vec<_> = hostport
        .to_socket_addrs()
        .map_err(|e| e.to_string())?
        .collect();

    if socket_addrs.is_empty() {
        Err("No addresses found.".to_string())
    } else {
        socket_addrs
            .choose(&mut rng) // Might be multihomed
            .cloned()
            .ok_or_else(|| "Random selection failed.".to_string())
    }
}

struct TestHttpServer {
    acceptor: Option<JoinHandle<Result<()>>>,
}

async fn fake_server_dispatch(
    req: hyper::Request<Incoming>,
) -> std::result::Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync + 'static>>
{
    let mut res = Response::builder().status(StatusCode::OK);

    let ct = content_type(Some(req.headers()))?;
    let body = req.collect().await?;
    let data: Vec<u8> = body.to_bytes().to_vec();
    res = res.header(
        hyper::header::CONTENT_TYPE,
        ct.unwrap_or(mime::TEXT_PLAIN).to_string(),
    );

    let body = Full::new(Bytes::from(data));
    Ok(res.body(body)?)
}

impl TestHttpServer {
    fn new(raw_url: String) -> Self {
        let mut instance = TestHttpServer { acceptor: None };
        instance.acceptor = Some(spawn(async move {
            let url: Url<HttpDefaults> = Url::parse(&raw_url)?;
            let host = url.host_str().unwrap_or("localhost");
            let port = url.port().unwrap_or(666);
            let hostport = format!("{host}:{port}");
            let addr = host_to_sockaddr(&hostport).unwrap();
            if "https" == url.scheme() {
                todo!();
                // let cert_file = "./tests/localhost.cert";
                // let key_file = "./tests/localhost.key";
                // setup_for_tls(); // Setups up TLS certs for localhost testing as a side-effect

                // let mut endpoint = tide::Server::new();
                // endpoint.at("/").all(fake_server_dispatch);
                // endpoint.at("/*").all(fake_server_dispatch);
                // if let Err(e) = endpoint
                //     .listen(
                //         TlsListener::build()
                //             .config(rustls::ServerConfig::new(NoClientAuth::new()))
                //             .addrs(url.url().socket_addrs(|| None)?[0])
                //             .cert(cert_file)
                //             .key(key_file),
                //     )
                //     .await
                // {
                //     error!("Error listening on {url}: {e}");
                // }
            } else {
                let listener = TcpListener::bind(addr).await?;
                let (tcp_stream, _remote_addr) = listener.accept().await?;
                let io = TokioIo::new(tcp_stream);

                tokio::task::spawn(async move {
                    let service = service_fn(fake_server_dispatch);

                    if let Err(err) = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .await
                    {
                        error!("Error serving connection: {err}");
                    }
                })
            };
            Ok(())
        }));
        instance
    }

    fn stop(&mut self) {
        if let Some(acceptor) = self.acceptor.take() {
            acceptor.abort();
        }
    }
}

// Convenience template for a round trip HTTP request/response interaction
async fn rtt(
    scheme: &'static str,
    target: &str,
    default_codec: &'static str,
    auth: Option<Value<'static>>,
    event: Event,
) -> Result<ValueAndMeta<'static>> {
    let url = format!("{scheme}://{target}");
    let mut config = literal!({
        "url": url.clone(),
        "method": "GET",
        "mime_mapping": {
            "application/json": {"name": "json", "config": {"mode": "sorted"}},
            "application/yaml": {"name": "yaml"},
            "*/*": default_codec,
        },
    });
    if let Some(auth) = auth {
        config.try_insert("auth", auth.clone_static());
    }
    let defn = literal!({
      "config": config,
    });
    let mut fake = TestHttpServer::new(url.clone());
    let mut harness = Harness::new("test", &http_impl::client::Builder::default(), &defn).await?;
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    harness.send_to_sink(event).await?;
    let event = harness.out()?.get_event().await;
    let event = match event {
        Ok(event) => event,
        Err(_) => harness.err()?.get_event().await?,
    };
    fake.stop();
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

#[tokio::test(flavor = "multi_thread")]
async fn http_client_request_with_defaults() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await?;
    let event = Event {
        data: (
            literal!(null),
            literal!({
                "http_client": {
                    "request": {},
                },
                "correlation": "http_client_request_with_defaults"
            }),
        )
            .into(),
        transactional: true,
        ..Default::default()
    };
    let res = rtt("http", &target, "string", None, event).await?;
    assert_eq!(&Value::from(""), res.value());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_client_request_with_defaults_post() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await?;
    let event = Event {
        data: (
            literal!("a string"),
            literal!({
                "http_client": {
                    "request": {"method": "POST"},
                },
                "correlation": "http_client_request_with_defaults_post"
            }),
        )
            .into(),
        transactional: true,
        ..Default::default()
    };
    let res = rtt("http", &target, "string", None, event).await?;
    assert_eq!(&Value::from("a string"), res.value());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_client_request_override_method() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await?;
    let event = Event {
        data: (
            Value::from(""),
            literal!({
                "http_client": {
                    "request": {
                        "method": "GET",
                    }
                },
                "correlation": "http_client_request_override_method"
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

#[tokio::test(flavor = "multi_thread")]
async fn http_client_request_override_endpoint() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await?;
    let event = Event {
        data: (
            Value::const_null(),
            literal!({
                "http_client": {
                    "request": {
                        "method": "PUT",
                        "url": format!("http://{target}/snot/badger?flork=mork")
                    }
                },
                "correlation": "http_client_request_override_endpoint"
            }),
        )
            .into(),
        ..Default::default()
    };
    let res = rtt("http", &target, "string", None, event).await?;

    let overriden_url: &str = &format!("http://{target}/snot/badger?flork=mork");
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(overriden_url), meta.get_str("uri"));
        assert_eq!(Some("PUT"), meta.get_str("method"));
    });

    assert_eq!(&Value::from("null"), res.value());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_client_request_override_codec() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await?;
    let event = Event {
        data: (
            literal!({ "snot": "badger" }),
            literal!({
                "http_client": {
                    "request": {
                        "method": "PATCH",
                        "headers": {
                            "content-type": "application/yaml"
                        }
                    }
                },
                "correlation": "http_client_request_override_codec"
            }),
        )
            .into(),
        ..Default::default()
    };
    let res = rtt("http", &target, "json", None, event).await?;

    let base_url: &str = &format!("http://{target}/");
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("uri"));
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

#[tokio::test(flavor = "multi_thread")]
async fn http_client_request_override_headers() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await?;
    let event = Event {
        data: (
            Value::from(42),
            literal!({
                "http_client": {
                    "request": {
                        "method": "PATCH",
                        "headers": { "x-snot": [ "badger", "badger", "badger"] }
                    }
                },
                "correlation": "http_client_request_override_headers"
            }),
        )
            .into(),
        ..Default::default()
    };
    let res = rtt("http", &target, "string", None, event).await?;

    let base_url: &str = &format!("http://{target}/");
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("uri"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
    });
    assert_eq!(
        Some(&Value::from("http_client_request_override_headers")),
        res.meta().get("correlation")
    );

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["badger", "badger", "badger"])),
            meta.get("x-snot")
        );
    });

    let length = res.meta().get("http_client").unwrap().get("content-length");
    assert_eq!(Some(&literal!(2)), length);

    assert_eq!(Value::from("42"), res.value());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_client_request_override_content_type() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await?;
    let event = Event {
        data: (
            literal!([{"snot": "badger"}, 42.0]),
            literal!({
                "http_client": {
                    "request": {
                        "method": "PATCH",
                        "headers": { "content-type": [ "application/json"] }
                    }
                },
                "correlation": "http_client_request_override_content_type"
            }),
        )
            .into(),
        transactional: true,
        ..Default::default()
    };
    let res = rtt("http", &target, "string", None, event).await?;

    let base_url: &str = &format!("http://{target}/");
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("uri"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
    });

    assert_eq!(
        Some(&Value::from("http_client_request_override_content_type")),
        res.meta().get("correlation")
    );

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["application/json"])),
            meta.get("content-type")
        );
    });

    assert_with_response_headers!(res, meta, {
        // assert_eq!(Some(&literal!(["24"])), meta.get("content-length")); - NOTE - changes in hyper from 0.14 - 1x - see below for alternate
        assert_eq!(
            Some(&literal!(["application/json"])),
            meta.get("content-type")
        );
    });

    let length = res.meta().get("http_client").unwrap().get("content-length");
    assert_eq!(Some(&literal!(24)), length);

    assert_eq!(
        literal!([
            {"snot": "badger"},
            42.0
        ]),
        res.value()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_client_request_auth_none() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await?;
    let event = Event {
        data: (
            literal!({"snot": "badger"}),
            literal!({
                "http_client": {
                    "request": {
                        "method": "PATCH",
                        "headers": { "Content-type": [ "application/json"] }
                    }
                },
                "correlation": "http_client_request_auth_none"
            }),
        )
            .into(),
        transactional: true,
        ..Default::default()
    };
    let res = rtt("http", &target, "string", Some(literal!("none")), event).await?;

    let base_url: &str = &format!("http://{target}/");
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("uri"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
    });

    assert_eq!(
        Some(&Value::from("http_client_request_auth_none")),
        res.meta().get("correlation")
    );

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["application/json"])), // NOTE - connector does not respect this and uses codec instead - see below for alternate
            meta.get(hyper::header::CONTENT_TYPE.as_str())
        );
    });

    assert_with_response_headers!(res, meta, {
        // assert_eq!(Some(&literal!(["17"])), meta.get("content-length")); NOTE - changes in hyper from 0.14 - 1x - see below for alternate
        // the server mirrors the request content-type
        assert_eq!(
            Some(&literal!(["application/json"])),
            meta.get(hyper::header::CONTENT_TYPE.as_str())
        );
    });

    let length = res.meta().get("http_client").unwrap().get("content-length");
    assert_eq!(Some(&literal!(17)), length);

    assert_eq!(
        literal!({
            "snot": "badger"
        }),
        res.value()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_client_request_auth_basic() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await?;
    let event = Event {
        data: (
            literal!({"snot": "badger"}),
            literal!({
                "http_client": {
                    "request": {
                        "method": "PATCH",
                        "headers": { "content-type": ["application/json"] }
                    }
                },
                "correlation": "http_client_request_auth_basic"
            }),
        )
            .into(),
        transactional: true,
        ..Default::default()
    };
    let res = rtt(
        "http",
        &target,
        "json",
        Some(literal!({
            "basic": {
                "username": "username-snot",
                "password": "username-badger"
            }
        })),
        event,
    )
    .await?;

    let base_url: &str = &format!("http://{target}/");
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("uri"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
    });
    assert_eq!(
        Some(&Value::from("http_client_request_auth_basic")),
        res.meta().get("correlation")
    );

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["application/json"])),
            meta.get("content-type")
        );
        assert_eq!(
            Some(&literal!([
                "Basic dXNlcm5hbWUtc25vdDp1c2VybmFtZS1iYWRnZXI="
            ])),
            meta.get("authorization")
        );
    });

    assert_with_response_headers!(res, meta, {
        // assert_eq!(Some(&literal!(["17"])), res.meta().get("content-length"));
        assert_eq!(
            Some(&literal!(["application/json"])),
            meta.get("content-type")
        );
    });

    let length = res.meta().get("http_client").unwrap().get("content-length");
    assert_eq!(Some(&literal!(17)), length);

    assert_eq!(literal!({"snot": "badger"}), res.value());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn chunked() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await?;
    let event = Event {
        data: (
            literal!([
                {
                    "data": {
                        "value": "chunk01 ",
                        "meta": {
                            "http_client": {
                                "request": {
                                    "method": "POST",
                                    "headers": {
                                        "content-TYPE": [ "text/plain"],
                                        "transfer-Encoding": "chunked"
                                    }
                                }
                            },
                            "correlation": "chunked"
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
            meta.get(hyper::header::TRANSFER_ENCODING.as_str())
        );
        assert_eq!(Some(&literal!(["text/plain"])), meta.get("content-type"));
    });
    // the fake server is setup to answer chunked requests with chunked responses
    assert_with_response_headers!(res, meta, {
        assert_eq!(
            // the fake server sends text/plain
            Some(&literal!(["text/plain"])),
            meta.get("content-type")
        );
    });
    // the quotes are artifacts from request json encoding
    assert_eq!(&Value::from("chunk01 chunk02 "), res.value());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_tls_config_https() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await?;
    let defn = literal!({
      "config": {
        "url": format!("https://{target}")
      },
      "codec": "influx",
    });
    let id = "test";
    let res = Harness::new(id, &http_impl::client::Builder::default(), &defn)
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();

    assert_eq!("[harness::test] Invalid definition: missing tls config with 'https' url. Set 'tls' to 'true' or provide a full tls config.", res);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_config() -> Result<()> {
    let defn = literal!({
      "codec": "binflux",
    });
    let id = "test";
    let res = Harness::new(id, &http_impl::client::Builder::default(), &defn)
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();

    assert!(res.contains("Missing Configuration"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn no_endpoint() -> Result<()> {
    let _ = env_logger::try_init();
    let target = find_free_tcp_endpoint_str().await?;
    let config = literal!({
        "url": format!("http://{target}"),
        "method": "GET",
        "mime_mapping": {
            "application/json": {"name": "json", "config": {"mode": "sorted"}},
            "application/yaml": {"name": "yaml"},
            "*/*": {"name": "string"},
        },
    });

    let defn = literal!({
      "config": config,
      "reconnect": {
        "retry": {
            "interval_ms": 100,
            "max_retries": 10,
            "growth_rate": 2.0
        }
      }
    });
    let mut harness = Harness::new("test", &http_impl::client::Builder::default(), &defn).await?;
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;
    // we use source_id 1 to ensure we differ from the default source_id 0
    let source_id = 1;
    // we use stream_id 1 to ensure we differ from the default stream_id 0
    let stream_id = 1;

    for i in 0..10 {
        let id = EventId::new(source_id, stream_id, i, DEFAULT_PULL_ID);

        let event = Event {
            id: id.clone(),
            transactional: true,
            ..Event::default()
        };
        harness.send_to_sink(event).await?;

        let (_, event) = loop {
            if let Some((port, event)) = harness.get_event()? {
                log::debug!("Got event in hartness from {port}");
                if event.id == id {
                    break (port, event);
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            harness.signal_tick_to_sink().await?;
        };

        assert_eq!(event.cb, CbAction::Fail);
    }
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
