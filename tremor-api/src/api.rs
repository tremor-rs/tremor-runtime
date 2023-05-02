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

// #![cfg_attr(coverage, no_coverage)]

use std::time::Duration;

use crate::errors::Error;
use http_types::{
    headers::{self, HeaderValue, ToHeaderValues},
    StatusCode,
};
use serde::{Deserialize, Serialize};
use tide::Response;
use tokio::task::JoinHandle;
use tremor_runtime::instance::State as InstanceState;
use tremor_runtime::system::World;

pub mod flow;
pub mod model;
pub mod prelude;
pub mod status;
pub mod version;

pub type Request = tide::Request<State>;
pub type Result<T> = std::result::Result<T, Error>;

/// Default API timeout applied to operations triggered by the API. E.g. get flow status
pub const DEFAULT_API_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct State {
    pub world: World,
}

#[derive(Clone, Copy, Debug)]
pub enum ResourceType {
    Json,
    Yaml,
    Trickle,
    Troy,
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}
impl ResourceType {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Yaml => "application/yaml",
            Self::Json => "application/json",
            Self::Trickle => "application/vnd.trickle",
            Self::Troy => "application/vnd.troy",
        }
    }
}

impl ToHeaderValues for ResourceType {
    type Iter = std::iter::Once<HeaderValue>;

    fn to_header_values(&self) -> http_types::Result<Self::Iter> {
        HeaderValue::from_bytes(self.as_str().as_bytes().to_vec()).map(std::iter::once)
    }
}

#[must_use]
pub fn content_type(req: &Request) -> Option<ResourceType> {
    match req
        .header(&headers::CONTENT_TYPE)
        .map(headers::HeaderValues::last)
        .map(headers::HeaderValue::as_str)
    {
        Some("application/yaml") => Some(ResourceType::Yaml),
        Some("application/json") => Some(ResourceType::Json),
        Some("application/vnd.trickle") => Some(ResourceType::Trickle),
        Some("application/vnd.troy") => Some(ResourceType::Troy),
        _ => None,
    }
}

#[must_use]
pub fn accept(req: &Request) -> ResourceType {
    // TODO implement correctly / RFC compliance
    match req
        .header(headers::ACCEPT)
        .map(headers::HeaderValues::last)
        .map(headers::HeaderValue::as_str)
    {
        Some("application/yaml") => ResourceType::Yaml,
        Some("application/vnd.trickle") => ResourceType::Trickle,
        Some("application/vnd.troy") => ResourceType::Troy,
        _ => ResourceType::Json,
    }
}

pub fn serialize<T: Serialize>(t: ResourceType, d: &T, code: StatusCode) -> Result<Response> {
    match t {
        ResourceType::Yaml => Ok(Response::builder(code)
            .header(headers::CONTENT_TYPE, t.as_str())
            .body(serde_yaml::to_string(d)?)
            .build()),
        ResourceType::Json => Ok(Response::builder(code)
            .header(headers::CONTENT_TYPE, t.as_str())
            .body(simd_json::to_string(d)?)
            .build()),
        _ => Err(Error::new(
            StatusCode::InternalServerError,
            format!("Unsuported formatting: {t}"),
        )),
    }
}

pub fn serialize_error(t: ResourceType, d: Error) -> Result<Response> {
    match t {
        ResourceType::Json | ResourceType::Yaml => serialize(t, &d, d.code),
        // formatting errors as trickle does not make sense so for this
        // fall back to the error's conversion into tide response
        ResourceType::Trickle | ResourceType::Troy => Ok(d.into()),
    }
}

pub fn reply<T: Serialize + Send + Sync + 'static>(
    req: &Request,
    result_in: T,
    ok_code: StatusCode,
) -> Result<Response> {
    serialize(accept(req), &result_in, ok_code)
}

async fn handle_api_request<
    G: std::future::Future<Output = Result<tide::Response>>,
    F: Fn(Request) -> G,
>(
    req: Request,
    handler_func: F,
) -> tide::Result {
    let resource_type = accept(&req);
    let path = req.url().path().to_string();
    let method = req.method();

    let r = match handler_func(req).await {
        Err(e) => {
            error!("[API {method} {path}] Error: {e}");
            Err(e)
        }
        Ok(r) => Ok(r),
    };

    r.or_else(|api_error| {
        serialize_error(resource_type, api_error).or_else(|e| Ok(Into::<tide::Response>::into(e)))
    })
}

/// server the tremor API in a separately spawned task
#[must_use]
pub fn serve(host: String, world: &World) -> JoinHandle<Result<()>> {
    let mut v1_app = tide::Server::with_state(State {
        world: world.clone(),
    });
    v1_app
        .at("/version")
        .get(|r| handle_api_request(r, version::get));
    v1_app
        .at("/status")
        .get(|r| handle_api_request(r, status::get_runtime_status));
    v1_app
        .at("/flows")
        .get(|r| handle_api_request(r, flow::list_flows));
    v1_app
        .at("/flows/:id")
        .get(|r| handle_api_request(r, flow::get_flow))
        .patch(|r| handle_api_request(r, flow::patch_flow_status));
    v1_app
        .at("/flows/:id/connectors")
        .get(|r| handle_api_request(r, flow::get_flow_connectors));
    v1_app
        .at("/flows/:id/connectors/:connector")
        .get(|r| handle_api_request(r, flow::get_flow_connector_status))
        .patch(|r| handle_api_request(r, flow::patch_flow_connector_status));

    let mut app = tide::Server::new();
    app.at("/v1").nest(v1_app);

    // spawn API listener
    tokio::task::spawn(async move {
        let res = app.listen(host).await;
        warn!("API stopped.");
        if let Err(e) = res {
            error!("API Error: {}", e);
            Err(e.into())
        } else {
            Ok(())
        }
    })
}

#[cfg(test)]
mod tests {
    use http_types::Url;
    use simd_json::ValueAccess;
    use std::time::Instant;
    use tokio::net::TcpListener;
    use tremor_runtime::{
        errors::Result as RuntimeResult,
        instance::State as InstanceState,
        system::{ShutdownMode, WorldConfig},
    };
    use tremor_script::{aggr_registry, ast::DeployStmt, deploy::Deploy, FN_REGISTRY};
    use tremor_value::{literal, value::StaticValue};

    use crate::api::model::{ApiFlowStatusReport, PatchStatus};

    use super::*;

    #[allow(clippy::too_many_lines)] // this is a test
    #[tokio::test(flavor = "multi_thread")]
    async fn test_api() -> RuntimeResult<()> {
        let _: std::result::Result<_, _> = env_logger::try_init();
        let config = WorldConfig {
            debug_connectors: true,
        };
        let (world, world_handle) = World::start(config).await?;

        let free_port = {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let port = listener.local_addr()?.port();
            drop(listener);
            port
        };
        let host = format!("127.0.0.1:{free_port}");
        let api_handle = serve(host.clone(), &world);
        info!("Listening on: {}", host);

        let src = r#"
        define flow api_test
        flow
            define pipeline main
            pipeline
                select event from in into out;
            end;
            create pipeline main;

            define connector my_null from `null`;
            create connector my_null;

            connect /connector/my_null to /pipeline/main;
            connect /pipeline/main to /connector/my_null;
        end;
        deploy flow api_test;
        "#;
        let aggr_reg = aggr_registry();
        let deployable = Deploy::parse(&src, &*FN_REGISTRY.read()?, &aggr_reg)?;
        let deploy = deployable
            .deploy
            .stmts
            .into_iter()
            .find_map(|stmt| match stmt {
                DeployStmt::DeployFlowStmt(deploy_flow) => Some((*deploy_flow).clone()),
                _other => None,
            })
            .expect("No deploy in the given troy file");
        world.start_flow(&deploy).await?;

        // check the status endpoint
        let start = Instant::now();
        let client: surf::Client = surf::Config::new()
            .set_base_url(Url::parse(&format!("http://{host}/"))?)
            .try_into()
            .expect("Could not create surf client");

        let mut body = client
            .get("/v1/status")
            .await?
            .body_json::<StaticValue>()
            .await
            .map(StaticValue::into_value);
        while body.is_err() || !body.get_bool("all_running").unwrap_or_default() {
            if start.elapsed() > Duration::from_secs(2) {
                panic!("Timeout waiting for all flows to run: {body:?}");
            } else {
                body = client
                    .get("/v1/status")
                    .await?
                    .body_json::<StaticValue>()
                    .await
                    .map(StaticValue::into_value);
            }
        }
        assert_eq!(
            literal!({
                "flows": {
                    "running": 1_u64
                },
                "num_flows": 1_u64,
                "all_running": true
            }),
            body?
        );

        // check the version endpoint
        let body = client
            .get("/v1/version")
            .await?
            .body_json::<StaticValue>()
            .await?
            .into_value();
        assert!(body.contains_key("version"));
        assert!(body.contains_key("debug"));

        // list flows
        let body = client
            .get("/v1/flows")
            .await?
            .body_json::<Vec<ApiFlowStatusReport>>()
            .await?;
        assert_eq!(1, body.len());
        assert_eq!("api_test", body[0].alias.as_str());
        assert_eq!(InstanceState::Running, body[0].status);
        assert_eq!(1, body[0].connectors.len());
        assert_eq!(String::from("my_null"), body[0].connectors[0]);

        // get flow
        let res = client.get("/v1/flows/i_do_not_exist").await?;
        assert_eq!(StatusCode::NotFound, res.status());

        let body = client
            .get("/v1/flows/api_test")
            .await?
            .body_json::<ApiFlowStatusReport>()
            .await?;

        assert_eq!("api_test", body.alias.as_str());
        assert_eq!(InstanceState::Running, body.status);
        assert_eq!(1, body.connectors.len());
        assert_eq!(String::from("my_null"), body.connectors[0]);

        // patch flow status
        let body = client
            .patch("/v1/flows/api_test")
            .body_json(&PatchStatus {
                status: InstanceState::Paused,
            })?
            .await?
            .body_json::<ApiFlowStatusReport>()
            .await?;

        assert_eq!("api_test", body.alias.as_str());
        assert_eq!(InstanceState::Paused, body.status);
        assert_eq!(1, body.connectors.len());
        assert_eq!(String::from("my_null"), body.connectors[0]);

        // invalid patch
        let mut res = client
            .patch("/v1/flows/api_test")
            .body_json(&PatchStatus {
                status: InstanceState::Failed,
            })?
            .await?;
        assert_eq!(StatusCode::BadRequest, res.status());
        res.body_bytes().await?; // consume the body

        // resume
        let body = client
            .patch("/v1/flows/api_test")
            .body_json(&PatchStatus {
                status: InstanceState::Running,
            })?
            .await?
            .body_json::<ApiFlowStatusReport>()
            .await?;

        assert_eq!("api_test", body.alias.as_str());
        assert_eq!(InstanceState::Running, body.status);
        assert_eq!(1, body.connectors.len());
        assert_eq!(String::from("my_null"), body.connectors[0]);

        // list flow connectors
        let body = client
            .get("/v1/flows/api_test/connectors")
            .await?
            .body_json::<StaticValue>()
            .await?
            .into_value();
        assert_eq!(
            literal!([
                {
                    "alias": "my_null",
                    "status": "running",
                    "connectivity": "connected",
                    "pipelines": {
                        "out": [
                            {
                                "port":  "in",
                                "alias": "main"
                            }
                        ],
                        "in": [
                            {
                                "port": "out",
                                "alias": "main"
                            }
                        ]
                    }
                }
            ]),
            body
        );

        // get flow connector
        let mut res = client
            .get("/v1/flows/api_test/connectors/i_do_not_exist")
            .await?;
        assert_eq!(StatusCode::NotFound, res.status());
        res.body_bytes().await?; //consume body

        let body = client
            .get("/v1/flows/api_test/connectors/my_null")
            .await?
            .body_json::<StaticValue>()
            .await?
            .into_value();
        assert_eq!(
            literal!({
                "alias": "my_null",
                "status": "running",
                "connectivity": "connected",
                "pipelines": {
                    "out": [
                        {
                            "port":  "in",
                            "alias": "main"
                        }
                    ],
                    "in": [
                        {
                            "port": "out",
                            "alias": "main"
                        }
                    ]
                }
            }),
            body
        );

        // patch flow connector status
        let body = client
            .patch("/v1/flows/api_test/connectors/my_null")
            .body_json(&PatchStatus {
                status: InstanceState::Paused,
            })?
            .await?
            .body_json::<StaticValue>()
            .await?
            .into_value();

        assert_eq!(
            literal!({
                "alias": "my_null",
                "status": "paused",
                "connectivity": "connected",
                "pipelines": {
                    "out": [
                        {
                            "port":  "in",
                            "alias": "main"
                        }
                    ],
                    "in": [
                        {
                            "port": "out",
                            "alias": "main"
                        }
                    ]
                }
            }),
            body
        );

        // invalid patch
        let mut res = client
            .patch("/v1/flows/api_test/connectors/my_null")
            .body_json(&PatchStatus {
                status: InstanceState::Failed,
            })?
            .await?;
        assert_eq!(StatusCode::BadRequest, res.status());
        res.body_bytes().await?; // consume the body

        // resume
        let body = client
            .patch("/v1/flows/api_test/connectors/my_null")
            .body_json(&PatchStatus {
                status: InstanceState::Running,
            })?
            .await?
            .body_json::<StaticValue>()
            .await?
            .into_value();
        assert_eq!(
            literal!({
                "alias": "my_null",
                "status": "running",
                "connectivity": "connected",
                "pipelines": {
                    "out": [
                        {
                            "port":  "in",
                            "alias": "main"
                        }
                    ],
                    "in": [
                        {
                            "port": "out",
                            "alias": "main"
                        }
                    ]
                }
            }),
            body
        );

        // cleanup
        world.stop(ShutdownMode::Graceful).await?;
        world_handle.abort();
        api_handle.abort();
        Ok(())
    }
}
