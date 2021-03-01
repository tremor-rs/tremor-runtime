// Copyright 2020-2021, The Tremor Team
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

use super::{
    nana::StatusCode,
    prelude::{close, destructurize, structurize, NetworkProtocol, StreamId},
    NetworkCont,
};
use crate::errors::Result;
use crate::repository::ArtefactId;
use crate::repository::BindingArtefact;
use crate::system::Conductor;
use crate::url::{ResourceType, TremorURL};
use crate::{config, network::control::ControlProtocol};
use hashbrown::HashMap as OtherMap;
use tremor_pipeline::{query::Query, Event, FN_REGISTRY};
use tremor_value::json;
use tremor_value::Value;

#[derive(Clone)]
pub(crate) struct ApiProtocol {
    alias: String,
    conductor: Conductor,
}

#[derive(Debug, PartialEq)]
enum ApiOperationKind {
    List,
    Fetch,
    Create,
    Instance,
    Activate,
    Deactivate,
    Remove,
}

#[derive(Debug)]
struct ApiRequest {
    op: ApiOperationKind,
    kind: ResourceType,
    name: Option<String>,
    instance: Option<String>,
}

impl ApiProtocol {
    pub(crate) fn new(ns: &mut ControlProtocol, alias: String, _headers: Value) -> Self {
        Self {
            alias,
            conductor: ns.conductor.clone(),
        }
    }
}

fn parse_operation<'a>(
    alias: &str,
    request: &'a mut Value<'a>,
) -> Result<(ApiRequest, &'a Value<'a>)> {
    if let Value::Object(c) = request {
        if let Some(cmd) = c.get(alias) {
            if let Value::Object(o) = cmd {
                // Required: `op`
                let op = if let Some(Value::String(s)) = o.get("op") {
                    let s: &str = &s.to_string();
                    match s {
                        "list" => ApiOperationKind::List,
                        "create" => ApiOperationKind::Create,
                        "instance" => ApiOperationKind::Instance,
                        "activate" => ApiOperationKind::Activate,
                        "deactivate" => ApiOperationKind::Deactivate,
                        "fetch" => ApiOperationKind::Fetch,
                        "remove" => ApiOperationKind::Remove,
                        unsupported_operation => {
                            let reason: &str = &format!(
                                "Invalid API operation - unsupported operation type `{}` - must be one of list, fetch, create, instance, activate, deactivate, remove",
                                unsupported_operation
                            );
                            return Err(reason.into());
                        }
                    }
                } else {
                    return Err(
                        "Required API command record field `op` (string) not found error".into(),
                    );
                };

                // Optional: `name`
                let name = if let Some(Value::String(s)) = o.get("name") {
                    Some(s.to_string())
                } else {
                    None
                };

                // Optional: `instance`
                let instance = if let Some(Value::String(s)) = o.get("instance") {
                    Some(s.to_string())
                } else {
                    None
                };

                // Required: Artefact type
                let kind = if let Some(Value::String(s)) = o.get("kind") {
                    let s: &str = &s.to_string();
                    match s {
                        "onramp" => ResourceType::Onramp,
                        "offramp" => ResourceType::Offramp,
                        "pipeline" => ResourceType::Pipeline,
                        "binding" => ResourceType::Binding,
                        unsupported_artefact_kind => {
                            let reason: &str = &format!(
                                "Invalid API operation - unsupported artefact kind `{}`",
                                unsupported_artefact_kind
                            );
                            return Err(reason.into());
                        }
                    }
                } else {
                    return Err("Required API command record field `kind` (string) not found error - must be one of offramp, onramp, pipeline, binding"
.into());
                };

                // We return a request struct with the cmd
                // record as a tuple (req,cmd) so that commands
                // that require a request body can specialize
                // evaluation accordingly
                Ok((
                    ApiRequest {
                        op,
                        kind,
                        name,
                        instance,
                    },
                    cmd,
                ))
            } else {
                return Err("Expect API command value to be a record".into());
            }
        } else {
            return Err("Expected protocol alias outer record not found".into());
        }
    } else {
        return Err("Expected API request to be a record, it was not".into());
    }
}

fn parse_trickle(body: String) -> Result<Query> {
    let aggr_reg = tremor_script::registry::aggr();
    let module_path = tremor_script::path::load();

    let query = Query::parse(
        &module_path,
        &body,
        "api-proto.trickle", // FIXME consider headers?
        vec![],
        &*FN_REGISTRY.lock()?,
        &aggr_reg,
    )?;

    Ok(query)
}

impl ApiProtocol {
    async fn on_list(&mut self, request: &ApiRequest) -> Result<NetworkCont> {
        let artefacts: Result<Vec<ArtefactId>> = match request.kind {
            ResourceType::Onramp => self.conductor.repo.list_onramps().await,
            ResourceType::Offramp => self.conductor.repo.list_offramps().await,
            ResourceType::Pipeline => self.conductor.repo.list_pipelines().await,
            ResourceType::Binding => self.conductor.repo.list_bindings().await,
            _other => return close(StatusCode::API_OPERATION_INVALID),
        };

        match artefacts {
            Ok(artefacts) => {
                let artefacts: Vec<String> = artefacts.iter().map(|i| i.to_string()).collect();
                return Ok(NetworkCont::SourceReply(
                    event!({ self.alias.to_string(): { "list": artefacts } }),
                ));
            }
            Err(x) => {
                error!("List operation failed {}", x);
                return close(StatusCode::API_OPERATION_FAILED);
            }
        }
    }

    async fn on_fetch(&mut self, request: &ApiRequest) -> Result<NetworkCont> {
        if let Some(name) = &request.name {
            match request.kind {
                ResourceType::Onramp => {
                    let url = TremorURL::from_onramp_id(&name)?;
                    let artefact = self.conductor.repo.find_onramp(&url).await?;
                    if let Some(x) = artefact {
                        return Ok(NetworkCont::SourceReply(event!({
                            self.alias.to_string(): {
                              "fetch": destructurize(&x.artefact)?,
                            }
                        })));
                    } else {
                        return Ok(NetworkCont::Close(event!({
                            self.alias.to_string(): { "fetch": null}
                        })));
                    }
                }
                ResourceType::Offramp => {
                    let url = TremorURL::from_offramp_id(&name)?;
                    let artefact = self.conductor.repo.find_offramp(&url).await?;
                    if let Some(x) = artefact {
                        return Ok(NetworkCont::SourceReply(event!({
                            self.alias.to_string(): {
                              "fetch": destructurize(&x.artefact)?,
                            }
                        })));
                    } else {
                        return Ok(NetworkCont::Close(event!({
                            self.alias.to_string(): { "fetch": null}
                        })));
                    }
                }
                ResourceType::Pipeline => {
                    let url = TremorURL::from_pipeline_id(&name)?;
                    let artefact = self.conductor.repo.find_pipeline(&url).await?;
                    if let Some(x) = artefact {
                        // NOTE We simply return the pipeline source directly
                        let value: String = x.artefact.source().to_string();
                        return Ok(NetworkCont::SourceReply(event!({
                            self.alias.to_string(): {
                              "fetch": value,
                            }
                        })));
                    } else {
                        return Ok(NetworkCont::Close(event!({
                            self.alias.to_string(): { "fetch": null}
                        })));
                    }
                }
                ResourceType::Binding => {
                    let url = TremorURL::from_binding_id(&name)?;
                    let artefact = self.conductor.repo.find_binding(&url).await?;
                    if let Some(x) = artefact {
                        // NOTE We narrow down to the binding specification
                        return Ok(NetworkCont::SourceReply(event!({
                            self.alias.to_string(): {
                              "fetch": destructurize(&x.artefact.binding)?,
                            }
                        })));
                    } else {
                        return Ok(NetworkCont::Close(event!({
                            self.alias.to_string(): { "fetch": null}
                        })));
                    }
                }
                _other => {
                    return close(StatusCode::API_OPERATION_INVALID);
                }
            };
        }

        return close(StatusCode::API_OPERATION_INVALID);
    }

    async fn on_create<'a>(
        &mut self,
        request: &ApiRequest,
        cmd: &'a Value<'a>,
    ) -> Result<NetworkCont> {
        if let Some(name) = &request.name {
            match request.kind {
                ResourceType::Onramp => {
                    let url = TremorURL::from_onramp_id(&name)?;
                    let artefact = self.conductor.repo.find_onramp(&url).await?;
                    if let Some(_already_exists_error) = artefact {
                        return close(StatusCode::API_CONNECTOR_ALREADY_EXISTS);
                    } else {
                        if let Value::Object(fields) = cmd {
                            if let Some(body) = fields.get("body") {
                                let value: Result<config::OnRamp> = structurize(body.clone());

                                if let Err(_) = value {
                                    return close(StatusCode::API_BODY_RECORD_EXPECTED);
                                }

                                let result = self
                                    .conductor
                                    .repo
                                    .publish_onramp(&url, false, value?)
                                    .await?;
                                return Ok(NetworkCont::SourceReply(event!({
                                    self.alias.to_string(): {
                                      "create": destructurize(&result)?,
                                    }
                                })));
                            } else {
                                return close(StatusCode::API_RECORD_EXPECTED);
                            }
                        } else {
                            return close(StatusCode::API_OPERATION_FAILED);
                        }
                    }
                }
                ResourceType::Offramp => {
                    let url = TremorURL::from_offramp_id(&name)?;
                    let artefact = self.conductor.repo.find_offramp(&url).await?;
                    if let Some(_already_exists_error) = artefact {
                        return close(StatusCode::API_CONNECTOR_ALREADY_EXISTS);
                    } else {
                        if let Value::Object(fields) = cmd {
                            if let Some(body) = fields.get("body") {
                                let value: Result<config::OffRamp> = structurize(body.clone());

                                if let Err(_) = value {
                                    return close(StatusCode::API_BODY_RECORD_EXPECTED);
                                }

                                let result = self
                                    .conductor
                                    .repo
                                    .publish_offramp(&url, false, value?)
                                    .await?;
                                return Ok(NetworkCont::SourceReply(event!({
                                    self.alias.to_string(): {
                                      "create": destructurize(&result)?,
                                    }
                                })));
                            } else {
                                return close(StatusCode::API_BODY_RECORD_EXPECTED);
                            }
                        } else {
                            return close(StatusCode::API_BODY_FIELD_EXPECTED);
                        }
                    }
                }
                ResourceType::Pipeline => {
                    let url = TremorURL::from_pipeline_id(&name)?;
                    let artefact = self.conductor.repo.find_pipeline(&url).await?;
                    if let Some(_already_exists_error) = artefact {
                        return close(StatusCode::API_PROCESSING_ELEMENT_ALREADY_EXISTS);
                    } else {
                        if let Value::Object(fields) = cmd {
                            if let Some(body) = fields.get("body") {
                                let body = body.to_string();
                                let query = parse_trickle(body);

                                if let Err(_) = query {
                                    return close(StatusCode::API_STRING_EXPECTED);
                                }

                                self.conductor
                                    .repo
                                    .publish_pipeline(&url, false, query?)
                                    .await?;

                                return Ok(NetworkCont::SourceReply(event!({
                                    self.alias.to_string(): {
                                      "create": destructurize(&name)?,
                                    }
                                })));
                            } else {
                                return close(StatusCode::API_RECORD_EXPECTED);
                            }
                        } else {
                            return close(StatusCode::API_BODY_STRING_FIELD_EXPECTED);
                        }
                    }
                }
                ResourceType::Binding => {
                    let url = TremorURL::from_binding_id(&name)?;
                    let artefact = self.conductor.repo.find_binding(&url).await?;
                    if let Some(_already_exists_error) = artefact {
                        return close(StatusCode::API_BINDING_ALREADY_EXISTS);
                    } else {
                        if let Value::Object(fields) = cmd {
                            if let Some(body) = fields.get("body") {
                                let value: Result<config::BindingMap> = structurize(body.clone());

                                if let Err(_) = value {
                                    return close(StatusCode::API_BODY_RECORD_EXPECTED);
                                }

                                let result = self
                                    .conductor
                                    .repo
                                    .publish_binding(
                                        &url,
                                        false,
                                        BindingArtefact {
                                            binding: crate::Binding {
                                                id: name.into(),
                                                description: "".into(),
                                                links: value?,
                                            },
                                            mapping: None,
                                        },
                                    )
                                    .await?;
                                return Ok(NetworkCont::SourceReply(event!({
                                    self.alias.to_string(): {
                                      "create": destructurize(&result.binding)?,
                                    }
                                })));
                            } else {
                                return close(StatusCode::API_BODY_RECORD_EXPECTED);
                            }
                        } else {
                            return close(StatusCode::API_BODY_RECORD_EXPECTED);
                        }
                    }
                }
                _other => {
                    return close(StatusCode::API_OPERATION_INVALID);
                }
            };
        } else {
            return close(StatusCode::API_NAME_FIELD_EXPECTED);
        }
    }

    async fn on_instance(&mut self, request: &ApiRequest) -> Result<NetworkCont> {
        match (&request.name, &request.instance, request.kind) {
            (Some(name), Some(instance), ResourceType::Binding) => {
                let url = TremorURL::from_binding_id(&format!("{}/{}", &name, &instance))?;
                let artefact = self.conductor.reg.find_binding(&url).await?;
                if let Some(x) = artefact {
                    return Ok(NetworkCont::SourceReply(event!({
                        self.alias.to_string(): {
                          "instance": destructurize(&x.binding)?,
                        }
                    })));
                } else {
                    return Ok(NetworkCont::SourceReply(event!({
                        self.alias.to_string(): {
                          "instance": null,
                        }
                    })));
                }
            }
            _unsupported_artefact_kind => {
                return close(StatusCode::API_ARTEFACT_KIND_UNKNOWN);
            }
        }
    }

    async fn on_activate<'a>(
        &mut self,
        request: &ApiRequest,
        cmd: &'a Value<'a>,
    ) -> Result<NetworkCont> {
        match (&request.name, &request.instance, request.kind) {
            (Some(name), Some(instance), ResourceType::Binding) => {
                let url = TremorURL::from_binding_id(&format!("{}/{}", &name, &instance))?;
                if let Value::Object(fields) = cmd {
                    if let Some(body) = fields.get("body") {
                        let mappings: OtherMap<String, String> = structurize(body.clone())?;
                        let artefact = self.conductor.link_binding(&url, mappings).await;
                        if let Ok(x) = artefact {
                            return Ok(NetworkCont::SourceReply(event!({
                                self.alias.to_string(): {
                                  "activate": destructurize(&x.binding)?,
                                }
                            })));
                        } else {
                            return Ok(NetworkCont::SourceReply(event!({
                                self.alias.to_string(): {
                                  "activate": null,
                                }
                            })));
                        }
                    } else {
                        return Err(
                            "Expected API operation cmd to be a record value with `body` field"
                                .into(),
                        );
                    }
                } else {
                    return close(StatusCode::API_RECORD_EXPECTED);
                }
            }
            _unsupported_artefact_kind => {
                return close(StatusCode::API_ARTEFACT_KIND_UNKNOWN);
            }
        }
    }

    async fn on_deactivate(&mut self, request: &ApiRequest) -> Result<NetworkCont> {
        match (&request.name, &request.instance, request.kind) {
            (Some(name), Some(instance), ResourceType::Binding) => {
                let url = TremorURL::from_binding_id(&format!("{}/{}", &name, &instance))?;
                let artefact = self.conductor.unlink_binding(&url, OtherMap::new()).await;
                if let Ok(x) = artefact {
                    return Ok(NetworkCont::SourceReply(event!({
                        self.alias.to_string(): {
                          "deactivate": destructurize(&x.binding)?,
                        }
                    })));
                } else {
                    return Ok(NetworkCont::SourceReply(event!({
                        self.alias.to_string(): {
                          "deactivate": null,
                        }
                    })));
                }
            }
            _unsupported_artefact_kind => {
                return close(StatusCode::API_ARTEFACT_KIND_UNKNOWN);
            }
        }
    }

    async fn on_remove(&mut self, request: &ApiRequest) -> Result<NetworkCont> {
        if let Some(name) = &request.name {
            match request.kind {
                ResourceType::Onramp => {
                    let url = TremorURL::from_onramp_id(&name)?;
                    let artefact = self.conductor.repo.unpublish_onramp(&url).await;
                    if let Ok(x) = artefact {
                        return Ok(NetworkCont::SourceReply(event!({
                            self.alias.to_string(): {
                              "remove": destructurize(&x)?,
                            }
                        })));
                    }
                }
                ResourceType::Offramp => {
                    let url = TremorURL::from_offramp_id(&name)?;
                    let artefact = self.conductor.repo.unpublish_offramp(&url).await;
                    if let Ok(x) = artefact {
                        return Ok(NetworkCont::SourceReply(event!({
                            self.alias.to_string(): {
                              "remove": destructurize(&x)?,
                            }
                        })));
                    }
                }
                ResourceType::Pipeline => {
                    let url = TremorURL::from_pipeline_id(&name)?;
                    let artefact = self.conductor.repo.unpublish_pipeline(&url).await;
                    if let Ok(x) = artefact {
                        // NOTE We simply return the pipeline source directly
                        let value: String = x.source().to_string();
                        return Ok(NetworkCont::SourceReply(event!({
                            self.alias.to_string(): {
                              "remove": value,
                            }
                        })));
                    }
                }
                ResourceType::Binding => {
                    let url = TremorURL::from_binding_id(&name)?;
                    let artefact = self.conductor.repo.unpublish_binding(&url).await;
                    if let Ok(x) = artefact {
                        return Ok(NetworkCont::SourceReply(event!({
                            self.alias.to_string(): {
                              "remove": destructurize(&x.binding)?,
                            }
                        })));
                    }
                }
                _other => {
                    return close(StatusCode::API_OPERATION_INVALID);
                }
            };
        }

        // Fallthrough - not found
        return Ok(NetworkCont::SourceReply(event!({
            "api": { "remove": null },
        })));
    }
}

#[async_trait::async_trait]
impl NetworkProtocol for ApiProtocol {
    fn on_init(&mut self) -> Result<()> {
        trace!("Initializing API network protocol");
        Ok(())
    }

    async fn on_event(&mut self, _sid: StreamId, event: &Event) -> Result<NetworkCont> {
        trace!("Received API network protocol event");
        let cmd = event.data.parts().0;
        let alias: &str = self.alias.as_str();
        let (request, cmd) = parse_operation(alias, cmd)?;
        Ok(match request.op {
            ApiOperationKind::List => self.on_list(&request).await?,
            ApiOperationKind::Fetch => self.on_fetch(&request).await?,
            ApiOperationKind::Create => self.on_create(&request, cmd).await?,
            ApiOperationKind::Instance => self.on_instance(&request).await?,
            ApiOperationKind::Activate => self.on_activate(&request, cmd).await?,
            ApiOperationKind::Deactivate => self.on_deactivate(&request).await?,
            ApiOperationKind::Remove => self.on_remove(&request).await?,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    // use crate::errors::Error;
    use crate::system;
    use crate::{errors::Result, event, system::Conductor};
    use async_channel::bounded;
    use halfbrown::hashmap;
    use tremor_script::Value;
    use tremor_value::json;

    macro_rules! assert_crud_lifecycle {
        ($sut:ident, $kind:expr, $id:expr, $create_actual:tt, $create_expected:tt, $list_expected:tt, $fetch_expected:tt, $remove_expected:tt) => {{
            // Create
            let create_cmd: Value = json!({
                "api": {
                    "op": "create",
                    "kind": $kind,
                    "name": $id,
                    "body": $create_actual,
                }
            }).into();
            let create_event = event!(create_cmd);
            let create_ok = $sut
                .on_event(
                    0, // from stream id
                    &create_event,
                )
                .await;

            assert!(create_ok.is_ok());
            let cont = create_ok.clone().unwrap();

            if let NetworkCont::SourceReply(event) = cont {
                let expected: Value = json!(
                    {"api": { "create": json!($create_expected) }}
                ).into();
                assert_eq!(event.data.parts().0, &expected);
            } else {
                assert!(false, "Create event failed");
            }

            // List
            let list_cmd: Value = json!({
                "api": {
                    "op": "list",
                    "kind": $kind,
                    "name": $id,
                }
            }).into();
            let list_event = event!(list_cmd);
            let list_ok = $sut
                .on_event(
                    0, // from stream id
                    &list_event,
                )
                .await;

            assert!(list_ok.is_ok());
            let cont = list_ok.clone().unwrap();

            if let NetworkCont::SourceReply(event) = cont {
                let expected: Value = json!(
                    {"api": { "list": json!($list_expected) }}
                ).into();
                assert_eq!(event.data.parts().0, &expected);
            } else {
                assert!(false, "List event failed");
            }

            // Fetch
            let fetch_cmd: Value = json!({
                "api": {
                    "op": "fetch",
                    "kind": $kind,
                    "name": $id,
                }
            }).into();
            let fetch_event = event!(fetch_cmd);
            let fetch_ok = $sut
                .on_event(
                    0, // from stream id
                    &fetch_event,
                )
                .await;

            assert!(fetch_ok.is_ok());
            let cont = fetch_ok.clone().unwrap();

            if let NetworkCont::SourceReply(event) = cont {
                let expected: Value = json!(
                    {"api": { "fetch": json!($fetch_expected) }}
                ).into();
                assert_eq!(event.data.parts().0, &expected);
            } else {
                assert!(false, "Fetch event failed");
            }

            // Remove
            let remove_cmd: Value = json!({
                "api": {
                    "op": "remove",
                    "kind": $kind,
                    "name": $id,
                }
            }).into();
            let remove_event = event!(remove_cmd);
            let remove_ok = $sut
                .on_event(
                    0, // from stream id
                    &remove_event,
                )
                .await;

            assert!(remove_ok.is_ok());
            let cont = remove_ok.clone().unwrap();

            if let NetworkCont::SourceReply(event) = cont {
                let expected: Value = json!(
                    {"api": { "remove": json!($remove_expected) }}
                ).into();
                assert_eq!(event.data.parts().0, &expected);
            } else {
                assert!(false, "Remove event failed");
            }

            // List - validate removal
            let list_cmd: Value = json!({
                "api": {
                    "op": "list",
                    "kind": $kind,
                    "name": $id,
                }
            }).into();
            let list_event = event!(list_cmd);
            let list_ok = $sut
                .on_event(
                    0, // from stream id
                    &list_event,
                )
                .await;

            assert!(list_ok.is_ok());
            let cont = list_ok.clone().unwrap();

            if let NetworkCont::SourceReply(event) = cont {
                let expected: Value = json!(
                    {"api": { "list": json!([]) }}
                ).into();
                assert_eq!(event.data.parts().0, &expected);
            } else {
                assert!(false, "Remove check event failed");
            }
        }};
    }

    #[async_std::test]
    async fn api_ko_client_cmd() -> Result<()> {
        use crate::temp_network::ws::UrMsg;
        let (uring_tx, _) = bounded::<UrMsg>(1);
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx, uring_tx);
        let mut control = ControlProtocol::new(&conductor);
        let mut sut = ApiProtocol::new(
            &mut control,
            "api".into(),
            Value::Object(Box::new(hashmap! {})),
        );

        assert_eq!(Ok(()), sut.on_init());

        // Usability - Progressive refinement - blindly following errors leads to resolution

        let actual = sut.on_event(0, &event!({"api": "snot"})).await;
        assert_err!(actual, "Expect API command value to be a record");

        let actual = sut.on_event(0, &event!({"api": {}})).await;
        assert_err!(
            actual,
            "Required API command record field `op` (string) not found error"
        );

        let actual = sut.on_event(0, &event!({"api": { "op": 1}})).await;
        assert_err!(
            actual,
            "Required API command record field `op` (string) not found error"
        );

        let actual = sut.on_event(0, &event!({"api": { "op": "snot"}})).await;
        assert_err!(
            actual,
            "Invalid API operation - unsupported operation type `snot` - must be one of list, fetch, create, instance, activate, deactivate, remove"
        );

        // Simplest valid api cmd is `list`
        let actual = sut.on_event(0, &event!({"api": { "op": "list"}})).await;
        assert_err!(
            actual,
            "Required API command record field `kind` (string) not found error - must be one of offramp, onramp, pipeline, binding"
        );
        let actual = sut
            .on_event(0, &event!({"api": { "op": "list", "kind": 1}}))
            .await;
        assert_err!(
            actual,
            "Required API command record field `kind` (string) not found error - must be one of offramp, onramp, pipeline, binding"
        );
        let actual = sut
            .on_event(0, &event!({"api": { "op": "list", "kind": "onramp"}}))
            .await?;
        assert_cont!(actual, {"api": { "list": []}});

        // Artefact id required
        let actual = sut
            .on_event(0, &event!({"api": { "op": "fetch", "kind": "offramp"}}))
            .await?;
        assert_cont!(actual, {"tremor": { "close": "1.200: Network API Protocol operation not supported"}});

        // If an artefact isn't found ...
        for op in vec!["fetch", "remove"] {
            for kind in vec!["onramp", "offramp", "pipeline", "binding"] {
                let actual = sut
                .on_event(
                    0,
                    &event!({"api": { "op": op.to_string(), "kind": kind.to_string(), "name": "snot"}}),
                )
                .await?;
                assert_cont!(actual, {"api": { op.to_string(): null }});
            }
        }

        // Create pipeline - bad trickle
        let actual = sut
                .on_event(
                    0,
                    &event!({"api": { "op": "create", "kind": "pipeline", "name": "snot", "body": "bogus"}}),
                )
                .await?;
        assert_cont!(actual, {"tremor": { "close": "1.207: Network API Protocol -  string expected"}});

        // Create onramp - bad json
        let actual = sut
                .on_event(
                    0,
                    &event!({"api": { "op": "create", "kind": "onramp", "name": "snot", "body": "bogus"}}),
                )
                .await?;
        assert_cont!(actual, {"tremor": { "close": "1.201: Network API Protocol -  record expected for body"}});

        // Create offramp - bad json
        let actual = sut
                .on_event(
                    0,
                    &event!({"api": { "op": "create", "kind": "offramp", "name": "snot", "body": "bogus"}}),
                )
                .await?;
        assert_cont!(actual, {"tremor": { "close": "1.201: Network API Protocol -  record expected for body"}});

        // Create binding - bad json
        let actual = sut
                .on_event(
                    0,
                    &event!({"api": { "op": "create", "kind": "binding", "name": "snot", "body": "bogus"}}),
                )
                .await?;
        assert_cont!(actual, {"tremor": { "close": "1.201: Network API Protocol -  record expected for body"}});

        // TODO activate deactivate only for bindings

        Ok(())
    }

    #[async_std::test]
    async fn api_protocol_crud() -> Result<()> {
        use crate::temp_network::ws::UrMsg;
        let (uring_tx, _) = bounded::<UrMsg>(1);
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx, uring_tx);
        let mut control = ControlProtocol::new(&conductor);
        let mut sut = ApiProtocol::new(
            &mut control,
            "api".into(),
            Value::Object(Box::new(hashmap! {})),
        );

        assert_eq!(Ok(()), sut.on_init());

        assert_crud_lifecycle!(sut, "onramp", "metronome",
            { "linked": false, "type": "metronome", "description": "snot", "id": "metronome", "config": { "interval": 5000 } } ,
            { "linked": false, "type": "metronome", "description": "snot", "id": "metronome", "config": { "interval": 5000 }, "err_required": false,  },
            [ "tremor://localhost/onramp/metronome" ],
            { "linked": false, "type": "metronome", "description": "snot", "id": "metronome", "config": { "interval": 5000 }, "err_required": false,  },
            { "linked": false, "type": "metronome", "description": "snot", "id": "metronome", "config": { "interval": 5000 }, "err_required": false,  }
        );

        assert_crud_lifecycle!(sut, "offramp", "debug",
            { "linked": false, "type": "debug", "description": "snot", "id": "debug" } ,
            { "linked": false, "type": "debug", "description": "snot", "id": "debug" },
            [ "tremor://localhost/offramp/debug" ],
            { "linked": false, "type": "debug", "description": "snot", "id": "debug" },
            { "linked": false, "type": "debug", "description": "snot", "id": "debug" }
        );
        // FIXME assert `name` and `id` are same in impl TODO // OR remove `name` for `create` ...
        // FIXME why are internal defaults not returned? Compare with onramps

        assert_crud_lifecycle!(
            sut,
            "pipeline",
            "identity",
            "select event from in into out",
            "identity",
            ["tremor://localhost/pipeline/identity"],
            "select event from in into out\n",
            "select event from in into out\n"
        );

        Ok(())
    }

    #[async_std::test]
    async fn api_protocol_servant_lifecycle() -> Result<()> {
        // TODO FIXME
        Ok(())
    }
}
