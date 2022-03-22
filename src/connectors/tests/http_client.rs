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

use super::{find_free_tcp_port, setup_for_tls, ConnectorHarness};
use crate::{
    connectors::{prelude::Url, utils::url::HttpDefaults},
    errors::Result,
};
use async_std::task::{spawn, JoinHandle};
use http_types::Method;
use log::error;
use rustls::NoClientAuth;
use std::time::Duration;
use tide;
use tide_rustls::TlsListener;
use tremor_common::url::ports::IN;
use tremor_pipeline::Event;
use tremor_pipeline::EventId;
use tremor_script::{literal, Value, ValueAndMeta};
use value_trait::ValueAccess;

/// Find free TCP host:port for use in test server endpoints
pub(crate) async fn find_free_tcp_endpoint_str() -> String {
    let port = find_free_tcp_port().await.to_string();
    format!("{}:{}", "localhost", port) // NOTE we use localhost rather than an IP for cmopat with TLS
}

struct TestHttpServer {
    acceptor: Option<JoinHandle<Result<()>>>,
}

async fn fake_server_dispatch(req: tide::Request<()>) -> tide::Result<tide::Response> {
    use tide::StatusCode;
    match &req.method() {
        Method::Get => {
            let mut res = tide::Response::new(StatusCode::Ok);
            res.set_body("get".to_string());
            Ok(res)
        }
        Method::Post => {
            let mut res = tide::Response::new(StatusCode::Ok);
            res.set_body("post".to_string());
            Ok(res)
        }
        Method::Put => {
            let mut res = tide::Response::new(StatusCode::Ok);
            res.set_body("put".to_string());
            Ok(res)
        }
        Method::Patch => {
            let mut res = tide::Response::new(StatusCode::Ok);
            res.set_body("patch".to_string());
            Ok(res)
        }
        _otherwise => {
            let mut res = tide::Response::new(StatusCode::BadRequest);
            res.set_body("ko".to_string());
            Ok(res)
        }
    }
}

impl TestHttpServer {
    async fn new(raw_url: String) -> Result<Self> {
        let mut instance = TestHttpServer { acceptor: None };
        instance.acceptor = Some(spawn(async move {
            // dbg!("Fake http server started");
            let url: Url<HttpDefaults> = Url::parse(&raw_url)?;
            if "https" == url.scheme() {
                dbg!("starting server on", &raw_url);
                let cert_file = "./tests/localhost.cert";
                let key_file = "./tests/localhost.key";
                setup_for_tls(); // Setups up TLS certs for localhost testing as a side-effect

                if let (Some(tcp_host), Some(tcp_port)) = (url.host_str(), url.port()) {
                    dbg!(&url);
                    let mut endpoint = tide::Server::new();
                    endpoint.at("/").all(fake_server_dispatch);
                    endpoint.at("/*").all(fake_server_dispatch);
                    endpoint
                        .listen(
                            TlsListener::build()
                                .config(rustls::ServerConfig::new(NoClientAuth::new()))
                                .addrs(&format!("{}:{}", tcp_host, tcp_port))
                                .cert(cert_file)
                                .key(key_file),
                        )
                        .await?;
                } else {
                    error!("Unable to extract host:port/endopint information from url");
                }
            } else {
                if let (Some(tcp_host), Some(tcp_port)) = (url.host_str(), url.port()) {
                    let mut endpoint = tide::Server::new();
                    endpoint.at("/").all(fake_server_dispatch);
                    endpoint.at("/*").all(fake_server_dispatch);
                    endpoint
                        .listen(&format!("{}:{}", tcp_host, tcp_port))
                        .await?;
                } else {
                    error!("Unable to extract host:port/endopint information from url");
                }
            };
            Ok(())
        }));
        Ok(instance)
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(_acceptor) = self.acceptor.take() {
            // dbg!("Fake http server stopped");
        }
        Ok(())
    }
}

// Convenience template for a round trip HTTP request/response interaction
async fn rtt(
    scheme: &str,
    target: &str,
    codec: &str,
    meta: Value<'static>,
) -> Result<ValueAndMeta<'static>> {
    let _ = env_logger::try_init();

    let defn = literal!({
      "id": "my_http_client",
      "type": "http_client",
      "config": {
        "url": format!("{}://{}", scheme, target),
        "method": "get",
      },
      "codec": codec.to_string(),
    });

    let mut fake = TestHttpServer::new(format!("{}://{}", scheme, target)).await?;

    let harness = ConnectorHarness::new("http_client", &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of connector");

    harness.start().await?;
    harness
        .wait_for_connected(Duration::from_millis(100))
        .await?;

    let meta = literal!({ "request": meta });
    let data = literal!(null);
    let echo_back = Event {
        id: EventId::default(),
        data: (data, meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(echo_back, IN).await?;

    let event = out_pipeline.get_event().await?;
    fake.stop().await?;
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    let (value, meta) = event.data.parts();
    Ok(ValueAndMeta::from_parts(
        value.clone_static(),
        meta.clone_static(),
    ))
}

macro_rules! assert_with_request_meta {
    ($res: expr, $meta: ident, $ctx: block) => {
        let rqm = $res.meta().get("request");
        if let Some($meta) = rqm {
            $ctx
        } else {
            assert!(false, "Expected request metadata to be set by connector",);
        }
    };
}

macro_rules! assert_with_response_headers {
    ($res: expr, $meta: ident, $ctx: block) => {
        let rqm = $res.meta().get("response");
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
        let rqm = $res.meta().get("request");
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
    let res = rtt("http", &target, "string", literal!({})).await?;
    assert_eq!("get", res.value().to_string());
    Ok(())
}

#[async_std::test]
async fn http_client_request_override_method() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let res = rtt("http", &target, "string", literal!({ "method": "post" })).await?;
    assert_eq!("post", res.value().to_string());
    Ok(())
}

#[async_std::test]
async fn http_client_request_override_endpoint() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let res = rtt(
        "http",
        &target,
        "string",
        literal!({ "method": "put", "url": format!("http://{}/snot/badger?flork=mork", target) }),
    )
    .await?;

    let overriden_url: &str = &format!("http://{}/snot/badger?flork=mork", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(overriden_url), meta.get_str("url"));
        assert_eq!(Some("PUT"), meta.get_str("method"));
    });

    assert_eq!("put", res.value().to_string());
    Ok(())
}

#[async_std::test]
async fn http_client_request_override_codec() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let res = rtt("http", &target, "json", literal!({ "method": "patch"})).await?;

    let base_url: &str = &format!("http://{}/", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("url"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
        assert_eq!(Some("json"), meta.get_str("codec"));
    });

    assert_eq!(literal!({"status": 415}), res.value());
    Ok(())
}

#[async_std::test]
async fn http_client_request_override_headers() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let res = rtt(
        "http",
        &target,
        "string",
        literal!({ "method": "patch", "headers": { "x-snot": [ "badger", "badger", "badger"] }}),
    )
    .await?;

    let base_url: &str = &format!("http://{}/", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("url"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
        assert_eq!(Some("string"), meta.get_str("codec"));
        assert_eq!(None, meta.get_str("correlation"));
    });

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["badger", "badger", "badger"])),
            meta.get("x-snot")
        );
    });

    assert_with_response_headers!(res, meta, {
        assert_eq!(Some(&literal!(["5"])), meta.get("content-length"));
    });

    assert_eq!("patch", res.value().to_string());
    Ok(())
}

#[async_std::test]
async fn http_client_request_override_content_type() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let res = rtt(
        "http",
        &target,
        "string",
        literal!({ "method": "patch", "headers": { "content-type": [ "application/json"] }}),
    )
    .await?;

    let base_url: &str = &format!("http://{}/", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("url"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
        assert_eq!(Some("string"), meta.get_str("codec"));
        assert_eq!(None, meta.get_str("correlation"));
    });

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["application/json"])), // NOTE - connector does not respect this and uses codec instead - see below for alternate
            meta.get("content-type")
        );
    });

    assert_with_response_headers!(res, meta, {
        assert_eq!(Some(&literal!(["5"])), meta.get("content-length"));
        assert_eq!(
            // NOTE: Arguably - content-type/codec should be driven by content-type in
            // the request if a header is provided, and fallback to a content-type
            // header in defaults, and then and only then fallback to codecs
            // for inferring the mime type
            //
            // Our legacy http connectors did not respect this or specify
            // a graceful degradation semantic, unfortunately
            Some(&literal!(["text/plain;charset=utf-8"])),
            meta.get("content-type")
        );
    });

    assert_eq!("patch", res.value().to_string());
    Ok(())
}

#[async_std::test]
async fn http_client_request_auth_none() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let res = rtt(
        "http",
        &target,
        "string",
        literal!({ "method": "patch", "headers": { "content-type": [ "application/json"], "auth": "none" }}),
    )
    .await?;

    let base_url: &str = &format!("http://{}/", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("url"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
        assert_eq!(Some("string"), meta.get_str("codec"));
        assert_eq!(None, meta.get_str("correlation"));
    });

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["application/json"])), // NOTE - connector does not respect this and uses codec instead - see below for alternate
            meta.get("content-type")
        );
    });

    assert_with_response_headers!(res, meta, {
        assert_eq!(Some(&literal!(["5"])), meta.get("content-length"));
        assert_eq!(
            // NOTE: Arguably - content-type/codec should be driven by content-type in
            // the request if a header is provided, and fallback to a content-type
            // header in defaults, and then and only then fallback to codecs
            // for inferring the mime type
            //
            // Our legacy http connectors did not respect this or specify
            // a graceful degradation semantic, unfortunately
            Some(&literal!(["text/plain;charset=utf-8"])),
            meta.get("content-type")
        );
    });

    assert_eq!("patch", res.value().to_string());
    Ok(())
}

#[async_std::test]
async fn http_client_request_auth_basic() -> Result<()> {
    let target = find_free_tcp_endpoint_str().await;
    let res = rtt(
        "http",
        &target,
        "string",
        literal!({
            "method": "patch", 
            "headers": { 
                "content-type": [ "application/json"]
            }, 
            "auth": { 
                "basic": { 
                    "username": "snot", "password": "badger" }}}),
    )
    .await?;

    let base_url: &str = &format!("http://{}/", target);
    assert_with_request_meta!(res, meta, {
        assert_eq!(Some(base_url), meta.get_str("url"));
        assert_eq!(Some("PATCH"), meta.get_str("method"));
        assert_eq!(Some("string"), meta.get_str("codec"));
        assert_eq!(None, meta.get_str("correlation"));
    });

    assert_with_request_headers!(res, meta, {
        assert_eq!(
            Some(&literal!(["application/json"])), // NOTE - connector does not respect this and uses codec instead - see below for alternate
            meta.get("content-type")
        );
        assert_eq!(
            Some(&literal!(["Basic c25vdDpiYWRnZXI="])),
            meta.get("authorization")
        );
    });

    assert_with_response_headers!(res, meta, {
        assert_eq!(Some(&literal!(["5"])), meta.get("content-length"));
        assert_eq!(
            // NOTE: Arguably - content-type/codec should be driven by content-type in
            // the request if a header is provided, and fallback to a content-type
            // header in defaults, and then and only then fallback to codecs
            // for inferring the mime type
            //
            // Our legacy http connectors did not respect this or specify
            // a graceful degradation semantic, unfortunately
            Some(&literal!(["text/plain;charset=utf-8"])),
            meta.get("content-type")
        );
    });

    assert_eq!("patch", res.value().to_string());
    Ok(())
}
