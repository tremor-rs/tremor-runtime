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
use tremor_pipeline::{Event, EventId};
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

async fn fake_server_dispatch(mut req: tide::Request<()>) -> tide::Result<tide::Response> {
    use tide::StatusCode;
    let mut res = tide::Response::new(StatusCode::Ok);
    dbg!(&req);
    let chunked = req
        .header(TRANSFER_ENCODING)
        .map(HeaderValues::last)
        .filter(|hv| hv.as_str() == "chunked")
        .is_some();

    let body = req.body_bytes().await?;

    if chunked {
        res.set_content_type(http_types::mime::PLAIN);
        res.set_body(Body::from_reader(Cursor::new(body), None));
    } else {
        if let Some(ct) = req.content_type() {
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
    target: String,
    codec: &'static str,
    meta: Value<'static>,
    data: Value<'static>,
    auth: Option<Value<'static>>,
    is_batch: bool,
) -> Result<ValueAndMeta<'static>> {
    let _ = env_logger::try_init();
    let url = format!("{}://{}", scheme, target);
    let mut config = literal!({
        "url": url.clone(),
        "method": "get",
    });
    if let Some(auth) = auth {
        config.try_insert("auth", auth.clone_static());
    }
    let defn = literal!({
      "id": "my_http_client",
      "type": "http_client",
      "config": config,
      "codec": codec.to_string(),
    });

    let mut fake = TestHttpServer::new(url.clone()).await?;

    let harness = ConnectorHarness::new(function_name!(), "http_client", &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of connector");

    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    let meta = literal!({
        "http_client": {
            "request": meta
        },
        "correlation": "snot"
    });
    let echo_back = Event {
        id: EventId::default(),
        data: (data, meta).into(),
        is_batch,
        ..Event::default()
    };
    harness.send_to_sink(echo_back, IN).await?;

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
    let res = rtt(
        "http",
        target.clone(),
        "string",
        literal!({}),
        literal!(null),
        None,
        false,
    )
    .await?;
    assert_eq!(&Value::from("null"), res.value());
    Ok(())
}

#[async_std::test]
async fn http_client_request_override_method() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let res = rtt(
        "http",
        target.clone(),
        "string",
        literal!({ "method": "get" }),
        Value::from(""),
        None,
        false,
    )
    .await?;
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
    let res = rtt(
        "http",
        target.clone(),
        "string",
        literal!({ "method": "put", "url": format!("http://{}/snot/badger?flork=mork", target) }),
        literal!(null),
        None,
        false,
    )
    .await?;

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
    let res = rtt(
        "http",
        target.clone(),
        "json",
        literal!({
            "method": "patch",
            "headers": {
                "content-type": "application/yaml"
            }
        }),
        literal!({ "snot": "badger" }),
        None,
        false,
    )
    .await?;

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
    let res = rtt(
        "http",
        target.clone(),
        "string",
        literal!({ "method": "patch", "headers": { "x-snot": [ "badger", "badger", "badger"] }}),
        literal!(42),
        None,
        false,
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
    let res = rtt(
        "http",
        target.clone(),
        "string",
        literal!({ "method": "patch", "headers": { "content-type": [ "application/json"] }}),
        literal!([{"snot": "badger"}, 42.0]),
        None,
        false,
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
    let res = rtt(
        "http",
        target.clone(),
        "string",
        literal!({ "method": "patch", "headers": { "Content-Type": ["application/json"]}}),
        literal!({"snot": "badger"}),
        Some(literal!("none")),
        false,
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
    let res = rtt(
        "http",
        target.clone(),
        "string",
        literal!({
            "method": "PATCh",
            "headers": {
                "content-TYPE": [ "application/json"]
            }
        }),
        literal!({
            "snot": "badger"
        }),
        Some(literal!({
            "basic": {
                "username": "snot",
                "password": "badger"
            }
        })),
        false,
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
    let res = rtt(
        "http",
        target.clone(),
        "string",
        literal!({}),
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
        None,
        true,
    )
    .await?;
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
