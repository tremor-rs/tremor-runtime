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
    nana::StatusCode, prelude::close, prelude::destructurize, NetworkCont, NetworkProtocol,
};
use crate::onramp;
use crate::pipeline;
use crate::url::TremorURL;
use crate::{errors::Result, url::ResourceType};
use crate::{network, registry::Registries};
use crate::{network::control::ControlProtocol, offramp};
use async_channel::{bounded, Receiver, Sender, TryRecvError};
use network::prelude::StreamId;
use simd_json::json;
use tremor_pipeline::Event;
use tremor_script::Value;

#[derive(Clone, Serialize)]
pub(crate) struct PubSubRegistry {
    pubs: Vec<TremorURL>,
    subs: Vec<TremorURL>,
}

impl PubSubRegistry {
    fn new() -> Self {
        Self {
            pubs: vec![],
            subs: vec![],
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum PubSubCommandPolicy {
    Strict,
    NonStrict,
}

impl std::fmt::Display for PubSubCommandPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                PubSubCommandPolicy::Strict => "strict",
                PubSubCommandPolicy::NonStrict => "non-strict",
            }
        )
    }
}

#[derive(Clone, Debug)]
struct PubSubHeaders {
    policy: PubSubCommandPolicy,
}

const PUBSUB_HEADER_COMMAND_POLICY: &'static str = "command.policy";

impl std::fmt::Display for PubSubHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", PUBSUB_HEADER_COMMAND_POLICY, self.policy)
    }
}

#[derive(Clone)]
pub(crate) struct PubSubProtocol {
    alias: String,
    reg: Registries,
    addr: network::Addr,
    ctrl: Receiver<network::ControlMsg>,
    rx: Receiver<network::Msg>,
    tx: Sender<network::Msg>,
    state: PubSubRegistry,
    headers: PubSubHeaders,
}

impl PubSubProtocol {
    fn is_strict(&self) -> bool {
        PubSubCommandPolicy::Strict == self.headers.policy
    }
}

#[derive(Debug, PartialEq)]
enum PubSubOperationKind {
    Subscribe,
    SubscribeForPublish,
    Send,
    Unsubscribe,
    UnsubscribeForPublish,
    List,
}

#[derive(Debug)]
struct PubSubOperation<'op> {
    op: PubSubOperationKind,
    body: Option<Value<'op>>,
}

fn parse_headers(headers: Value) -> PubSubHeaders {
    if let Value::Object(headers) = headers {
        let policy = match headers.get(PUBSUB_HEADER_COMMAND_POLICY) {
            Some(Value::String(policy_str)) => {
                dbg!(policy_str);
                match policy_str.to_lowercase().as_str() {
                    // Preferred
                    "strict" => PubSubCommandPolicy::Strict,
                    // Relaxed policy suitable for IDE integration, humans
                    "non-strict" => PubSubCommandPolicy::NonStrict,
                    // FIXME This should really be a fatal protocol error
                    _otherwise => PubSubCommandPolicy::Strict,
                }
            }
            _otherwise => PubSubCommandPolicy::Strict,
        };

        PubSubHeaders { policy }
    } else {
        // Strict by default - any runtime errors are fatal protocol errors
        PubSubHeaders {
            policy: PubSubCommandPolicy::Strict,
        }
    }
}

impl PubSubProtocol {
    pub(crate) fn new(ns: &ControlProtocol, alias: String, headers: Value) -> Self {
        let qsize = 10;
        let (tx, rx) = bounded::<network::Msg>(qsize);
        let (ctrl_tx, ctrl_rx) = bounded::<network::ControlMsg>(qsize);

        Self {
            headers: parse_headers(headers),
            alias,
            reg: ns.conductor.reg.clone(),
            addr: network::Addr::new(
                tx.clone(),
                ctrl_tx,
                0,
                TremorURL::from_network_id("/network/0").unwrap(),
            ), // FIXME id generator
            ctrl: ctrl_rx,
            rx,
            tx,
            state: PubSubRegistry::new(),
        }
    }
}

fn parse_operation<'op>(alias: &str, request: &'op mut Value<'op>) -> Result<PubSubOperation<'op>> {
    if let Value::Object(c) = request {
        if let Some(cmd) = c.get(alias) {
            if let Value::Object(o) = cmd {
                // Required: `op`
                let keys: Vec<String> = o.keys().into_iter().map(|f| f.to_string()).collect();

                if keys.len() != 1 {
                    return Err(
                        "PubSub record operations must contain exactly one field/key".into(),
                    );
                }
                let op: &str = &keys[0];

                // Optional: `body`
                let body = if let Some(data) = o.get(op) {
                    let data: Option<Value<'op>> = Some(data.clone_static());
                    data
                } else {
                    // FIXME consider making this an error condition
                    None
                };

                let op = match op {
                    "subscribe" => PubSubOperationKind::Subscribe,
                    "publish" => PubSubOperationKind::SubscribeForPublish,
                    "send" => PubSubOperationKind::Send,
                    "unsubscribe" => PubSubOperationKind::Unsubscribe,
                    "unpublish" => PubSubOperationKind::UnsubscribeForPublish,
                    unsupported_operation => {
                        let reason: &str = &format!(
                            "Invalid PubSub operation - unsupported record operation type `{}` - must be one of subscribe, publish, send, unsubscribe, unpublish, list",
                            unsupported_operation
                        );
                        return Err(reason.into());
                    }
                };

                return Ok(PubSubOperation { op, body });
            } else if let Value::String(o) = cmd {
                let check = o.to_string();
                if "list" == &check {
                    return Ok(PubSubOperation {
                        op: PubSubOperationKind::List,
                        body: None,
                    });
                } else {
                    return Err("Expect PubSub command value to be a record or `list`".into());
                }
            } else {
                return Err("Expect PubSub command value to be a record or `list`".into());
            }
        } else {
            return Err("Expected protocol alias outer record not found".into());
        }
    } else {
        return Err("Expected PubSub request to be a record, it was not".into());
    }
}

impl PubSubProtocol {
    // Given a network-unique, stream id, generate a node-unique tremor url
    fn uuid(&self, sid: StreamId) -> Result<TremorURL> {
        // FIXME Consider multiple network instances in a single node
        // FIXME Consider clustering and unique id's across multiple nodes in a cluster
        TremorURL::from_network_id(&format!("streams/{}/{}", sid, &self.alias))
    }

    async fn publish_pipeline(&mut self, sid: StreamId, url: TremorURL) -> Result<bool> {
        let pipeline = self.reg.find_pipeline(&url).await?;
        match pipeline {
            Some(hit) => {
                let source = self.uuid(sid)?;

                // Call to register with target as source
                hit.send_mgmt(pipeline::MgmtMsg::OpenTap(source, url, self.addr.clone()))
                    .await?;

                // Call-Response confirming subscription
                match self.ctrl.recv().await {
                    Ok(_response) => {
                        trace!("network pubsub publication ok");
                        Ok(true)
                    }
                    Err(_e) => {
                        error!("publication failure in pubsub");
                        Ok(false)
                    }
                }
            }
            None => Err("Pipeline not found".into()),
        }
    }

    async fn publish_offramp(&mut self, sid: StreamId, url: TremorURL) -> Result<bool> {
        let onramp = self.reg.find_offramp(&url).await?;
        match onramp {
            Some(hit) => {
                let source = self.uuid(sid)?;

                // Call to register with target as source
                hit.send(offramp::Msg::OpenTap(source, url, self.addr.clone()))
                    .await?;

                // Call-Response confirming subscription
                match self.ctrl.recv().await {
                    Ok(_response) => {
                        trace!("network pubsub subscription ok");
                        Ok(true)
                    }
                    Err(_e) => {
                        error!("subscription failure in pubsub");
                        Ok(false)
                    }
                }
            }
            None => Err("Offramp not found for publication".into()),
        }
    }

    async fn publish(&mut self, sid: StreamId, url: TremorURL) -> Result<bool> {
        match url.resource_type() {
            Some(ResourceType::Offramp) => self.publish_offramp(sid, url).await,
            Some(ResourceType::Pipeline) => self.publish_pipeline(sid, url).await,
            Some(_other) => {
                error!("Invalid subscribe-for-publish attempted to known but unsupported resource type");
                return Ok(false);
            }
            None => {
                error!("Invalid subscribe-for-publish attempted to unknown url resource type");
                return Ok(false);
            }
        }
    }

    async fn subscribe_onramp(&mut self, sid: StreamId, url: TremorURL) -> Result<bool> {
        let onramp = self.reg.find_onramp(&url).await?;
        match onramp {
            Some(hit) => {
                let source = self.uuid(sid)?;

                // Call to register with target as source
                hit.send(onramp::Msg::OpenTap(source, url, self.addr.clone()))
                    .await?;

                // Call-Response confirming subscription
                match self.ctrl.recv().await {
                    Ok(_response) => {
                        trace!("network pubsub subscription ok");
                        Ok(true)
                    }
                    Err(_e) => {
                        error!("subscription failure in pubsub");
                        Ok(false)
                    }
                }
            }
            None => {
                if PubSubCommandPolicy::Strict == self.headers.policy {
                    Err("Onramp not found".into())
                } else {
                    Ok(false) // We ignore not found onramps in non-strict
                }
            }
        }
    }

    async fn subscribe_pipeline(&mut self, sid: StreamId, url: TremorURL) -> Result<bool> {
        let pipeline = self.reg.find_pipeline(&url).await?;
        match pipeline {
            Some(hit) => {
                let source = self.uuid(sid)?;

                // Call to register with target as source
                hit.send_mgmt(pipeline::MgmtMsg::OpenTap(source, url, self.addr.clone()))
                    .await?;

                // Call-Response confirming subscription
                match self.ctrl.recv().await {
                    Ok(_response) => {
                        trace!("network pubsub subscription ok");
                        Ok(true)
                    }
                    Err(_e) => {
                        error!("subscription failure in pubsub");
                        Ok(false)
                    }
                }
            }
            None => {
                if PubSubCommandPolicy::Strict == self.headers.policy {
                    Err("Pipeline not found".into())
                } else {
                    Ok(false) // We ignore not found pipelines in non-strict
                }
            }
        }
    }

    async fn subscribe(&mut self, sid: StreamId, url: TremorURL) -> Result<bool> {
        match url.resource_type() {
            Some(ResourceType::Onramp) => self.subscribe_onramp(sid, url).await,
            Some(ResourceType::Pipeline) => self.subscribe_pipeline(sid, url).await,
            Some(_other) => {
                error!("Invalid subscription attempted to known but unsupported resource type");
                return Ok(false);
            }
            None => {
                error!("Invalid subscription attempted to unknown url resource type");
                return Ok(false);
            }
        }
    }

    async fn unsubscribe_onramp(&mut self, sid: StreamId, url: TremorURL) -> Result<bool> {
        let onramp = self.reg.find_onramp(&url).await?;

        match onramp {
            Some(hit) => {
                let source = self.uuid(sid)?;

                // Call to unregister with target as source
                hit.send(onramp::Msg::CloseTap {
                    source,
                    target: url,
                    ctrl: self.addr.clone(),
                })
                .await?;

                match self.ctrl.recv().await {
                    Ok(_response) => {
                        trace!("network pubsub unsubscription ok");
                        Ok(true)
                    }
                    Err(_e) => {
                        error!("unsubscription failure in pubsub");
                        Ok(false)
                    }
                }
            }
            None => {
                if PubSubCommandPolicy::Strict == self.headers.policy {
                    Err("Onramp not found".into())
                } else {
                    Ok(false) // We ignore not found onramps in non-strict
                }
            }
        }
    }

    async fn unsubscribe_pipeline(&mut self, sid: StreamId, url: TremorURL) -> Result<bool> {
        let pipeline = self.reg.find_pipeline(&url).await?;

        match pipeline {
            Some(hit) => {
                let source = self.uuid(sid)?;

                // Call to unregister with target as source
                hit.send_mgmt(pipeline::MgmtMsg::CloseTap {
                    source,
                    target: url,
                    addr: self.addr.clone(),
                })
                .await?;

                match self.ctrl.recv().await {
                    Ok(_response) => {
                        trace!("network pubsub unsubscription ok");
                        Ok(true)
                    }
                    Err(_e) => {
                        error!("unsubscription failure in pubsub");
                        Ok(false)
                    }
                }
            }
            None => {
                if PubSubCommandPolicy::Strict == self.headers.policy {
                    Err("Pipeline not found".into())
                } else {
                    Ok(false) // We ignore not found pipelines in non-strict
                }
            }
        }
    }

    async fn unsubscribe_offramp(&mut self, sid: StreamId, url: TremorURL) -> Result<bool> {
        let offramp = self.reg.find_offramp(&url).await?;

        match offramp {
            Some(hit) => {
                let source = self.uuid(sid)?;

                // Call to unregister with target as source
                hit.send(offramp::Msg::CloseTap {
                    source,
                    target: url,
                    addr: self.addr.clone(),
                })
                .await?;

                match self.ctrl.recv().await {
                    Ok(_response) => {
                        trace!("network pubsub unsubscription ok");
                        Ok(true)
                    }
                    Err(_e) => {
                        error!("unsubscription failure in pubsub");
                        Ok(false)
                    }
                }
            }
            None => {
                if PubSubCommandPolicy::Strict == self.headers.policy {
                    Err("Offramp not found".into())
                } else {
                    Ok(false) // We ignore not found offramps in non-strict
                }
            }
        }
    }

    async fn unsubscribe(&mut self, sid: StreamId, url: TremorURL) -> Result<bool> {
        match url.resource_type() {
            Some(ResourceType::Onramp) => self.unsubscribe_onramp(sid, url).await,
            Some(ResourceType::Pipeline) => self.unsubscribe_pipeline(sid, url).await,
            Some(ResourceType::Offramp) => self.unsubscribe_offramp(sid, url).await,
            Some(_other) => {
                error!("Invalid subscription attempted to known but unsupported resource type");
                return Ok(false);
            }
            None => {
                error!("Invalid subscription attempted to unknown url resource type");
                return Ok(false);
            }
        }
    }

    async fn send_pipeline(&mut self, _sid: StreamId, url: TremorURL, data: Event) -> Result<()> {
        let pipeline = self.reg.find_pipeline(&url).await?;

        match pipeline {
            Some(hit) => {
                let port = url.instance_port().unwrap().to_string(); // FIXME some/match

                hit.send(pipeline::Msg::Event {
                    input: port.into(), // FIXME ensure on subscribe port is present
                    event: data,
                })
                .await?;

                Ok(())
            }
            None => {
                if PubSubCommandPolicy::Strict == self.headers.policy {
                    Err("Cannot send, not subscribed for publication".into())
                } else {
                    Ok(()) // We ignore not found pipelines in non-strict
                }
            }
        }
    }

    async fn send_offramp(&mut self, _sid: StreamId, url: TremorURL, data: Event) -> Result<()> {
        let offramp = self.reg.find_offramp(&url).await?;

        match offramp {
            Some(hit) => {
                // let source = self.uuid(sid)?;

                hit.send(offramp::Msg::Event {
                    input: "network".into(), // FIXME needs a better virtual/fake port signifier
                    event: data,
                })
                .await?;

                Ok(())
            }
            None => {
                if PubSubCommandPolicy::Strict == self.headers.policy {
                    Err("Cannot send, not subscribed for publication".into())
                } else {
                    Ok(()) // We ignore not found offramps in non-strict
                }
            }
        }
    }

    async fn send(&mut self, sid: StreamId, to: String, data: Event) -> Result<()> {
        let url = TremorURL::parse(&to)?;

        match url.resource_type() {
            Some(ResourceType::Pipeline) => {
                self.send_pipeline(sid, url, data).await?;
            }
            Some(ResourceType::Offramp) => {
                self.send_offramp(sid, url, data).await?;
            }
            Some(_other) => {
                error!("Invalid send attempted to known but unsupported resource type");
                return Ok(());
            }
            None => {
                error!("Invalid send attempted to unknown url resource type");
                return Ok(());
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait()]
impl NetworkProtocol for PubSubProtocol {
    fn on_init(&mut self) -> Result<()> {
        trace!("Initializing pubsub for network protocol");
        Ok(())
    }

    async fn on_event(&mut self, sid: StreamId, event: &Event) -> Result<NetworkCont> {
        trace!("Received pubsub network protocol event");
        let cmd = event.data.parts().0;
        let alias: &str = self.alias.as_str();
        let request = parse_operation(alias, cmd)?;

        let mut subs_ok = vec![];
        let mut pubs_ok = vec![];
        match request.op {
            PubSubOperationKind::Subscribe => {
                // Subscribe for consumption
                if let Some(Value::Array(a)) = request.body {
                    for endpoint in a {
                        let url = TremorURL::parse(&endpoint.to_string())?;

                        match self.subscribe(sid, url.clone()).await {
                            Ok(true) => {
                                // FIXME check if already subscribed
                                self.state.subs.push(url.clone());
                                subs_ok.push(endpoint.to_string())
                            }
                            Ok(_) => (), // Ignore - modeled as acknowledging the empty set
                            Err(_error) => {
                                if self.is_strict() {
                                    return close(StatusCode::PUBSUB_SUBSCRIPTION_FAILED);
                                }
                            }
                        }
                    }
                }

                // Empty acks imply subscription failure for non-strict
                let subs_ok: Value = subs_ok.into();
                return Ok(NetworkCont::SourceReply(
                    event!({ self.alias.to_string(): { "subscribe-ack": subs_ok }}),
                ));
            }
            PubSubOperationKind::Unsubscribe => {
                if let Some(Value::Array(a)) = request.body {
                    for endpoint in a {
                        let url = TremorURL::parse(&endpoint.to_string())?;

                        // FIXME check if absent
                        let mut idx: i32 = -1;
                        for x in &self.state.subs {
                            idx += 1;
                            if x == &url {
                                break;
                            }
                        }

                        if (idx >= 0) {
                            self.state.subs.remove(idx as usize);
                        }

                        match self.unsubscribe(sid, url.clone()).await {
                            Ok(true) => {
                                // FIXME check if already subscribed
                                subs_ok.push(endpoint.to_string())
                            }
                            Ok(_) => (), // Ignore - modeled as acknowledging the empty set
                            Err(_error) => {
                                if self.is_strict() {
                                    return close(StatusCode::PUBSUB_UNSUBSCRIPTION_FAILED);
                                }
                            }
                        }
                    }
                }
                let subs_ok: Value = subs_ok.into();
                return Ok(NetworkCont::SourceReply(
                    event!({ self.alias.to_string(): { "unsubscribe-ack": subs_ok }}),
                ));
            }
            PubSubOperationKind::SubscribeForPublish => {
                // Subscribe for publication
                if let Some(Value::Array(a)) = request.body {
                    for endpoint in a {
                        let url = TremorURL::parse(&endpoint.to_string())?;

                        match self.publish(sid, url.clone()).await {
                            Ok(true) => {
                                // FIXME check if already subscribed
                                self.state.pubs.push(url.clone());
                                pubs_ok.push(endpoint.to_string())
                            }
                            Ok(_) => (), // Ignore - modeled as acknowledging the empty set
                            Err(_error) => {
                                if self.is_strict() {
                                    return close(StatusCode::PUBSUB_PUBLICATION_FAILED);
                                }
                            }
                        }
                    }
                }
                let pubs_ok: Value = pubs_ok.into();
                return Ok(NetworkCont::SourceReply(
                    event!({ self.alias.to_string(): { "publish-ack": pubs_ok }}),
                ));
            }
            PubSubOperationKind::UnsubscribeForPublish => {
                if let Some(Value::Array(a)) = request.body {
                    for endpoint in a {
                        let url = TremorURL::parse(&endpoint.to_string())?;

                        // FIXME check if absent
                        let mut idx: i32 = -1;
                        for x in &self.state.pubs {
                            idx += 1;
                            if x == &url {
                                break;
                            }
                        }

                        if (idx >= 0) {
                            self.state.pubs.remove(idx as usize);
                        }

                        match self.unsubscribe(sid, url.clone()).await {
                            Ok(true) => {
                                // FIXME check if already subscribed for publication
                                pubs_ok.push(endpoint.to_string())
                            }
                            Ok(_) => (), // Ignore - modeled as acknowledging the empty set
                            Err(_error) => {
                                if self.is_strict() {
                                    return close(StatusCode::PUBSUB_UNPUBLICATION_FAILED);
                                }
                            }
                        }
                    }
                }
                let pubs_ok: Value = pubs_ok.into();
                return Ok(NetworkCont::SourceReply(
                    event!({ self.alias.to_string(): { "unpublish-ack": pubs_ok }}),
                ));
            }
            PubSubOperationKind::Send => {
                if let Some(Value::Object(body)) = request.body {
                    if let Some(Value::String(to)) = body.get("to") {
                        if let Some(data) = body.get("data") {
                            self.send(sid, to.to_string(), event!(data)).await?;
                            return Ok(NetworkCont::None);
                        } else {
                            return Ok(NetworkCont::Close(
                                event!({"tremor": { "close": "cannot send as `data` missing" } }),
                            ));
                        }
                    } else {
                        return Ok(NetworkCont::Close(
                            event!({"tremor": { "close": "cannot send as `to` missing" } }),
                        ));
                    }
                } else {
                    return Ok(NetworkCont::Close(
                        event!({"tremor": { "close": "cannot send as `data` missing" } }),
                    ));
                }
            }
            PubSubOperationKind::List => {
                let value = destructurize(&self.state)?;
                return Ok(NetworkCont::SourceReply(
                    event!({ self.alias.to_string(): { "list": value }}),
                ));
            }
        }
    }

    async fn on_data(&mut self) -> Result<Option<Vec<Event>>> {
        let mut data = vec![];

        loop {
            match self.rx.try_recv() {
                Ok(network::Msg::Event { event, .. }) => {
                    let (value, meta) = event.data.parts();
                    data.push(
                        event!({ self.alias.to_string(): { "data": value , "meta": meta } } ),
                    );
                    continue;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Closed) => break, // FIXME TODO log an error, this case should never ordinarily occur
            }
        }

        if data.len() > 0 {
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use std::net::SocketAddr;

    use super::*;
    use crate::system;
    use crate::{errors::Result, event, system::Conductor};
    use async_channel::bounded;
    use halfbrown::hashmap;
    use network::api::ApiProtocol;
    use system::World;
    use tremor_script::Value;
    use tremor_value::json;

    async fn deploy(api: &mut ApiProtocol) -> Result<()> {
        let metro = api
            .on_event(
                0,
                &event!(
                    {
                        "api": {
                            "op": "create", "kind": "onramp", "name": "metronome",
                            "body": {
                                "linked": false,
                                "type": "metronome",
                                "description": "snot",
                                "id": "metronome",
                                "config": { "interval": 5000 } },
                        }
                    }
                ),
            )
            .await;
        assert!(metro.is_ok());
        assert_cont!(metro?, { "api":
            { "create": {
                    "config": { "interval": 5000 },
                    "linked": false,
                    "type": "metronome",
                    "description": "snot",
                    "id": "metronome",
                    "err_required": false,
                }
            }
        });

        let pipe = api
            .on_event(
                0,
                &event!(
                    {
                        "api": {
                            "op": "create", "kind": "pipeline", "name": "main",
                            "body": "select event from in into out",
                        }
                    }
                ),
            )
            .await;
        assert!(pipe.is_ok());

        let offramp = api
            .on_event(
                0,
                &event!(
                    {
                        "api": {
                            "op": "create", "kind": "offramp", "name": "debug",
                            "body": {
                                "linked": false,
                                "type": "debug",
                                "description": "snot",
                                "id": "debug"
                            }
                        }
                    }
                ),
            )
            .await;
        assert!(offramp.is_ok());

        let links = api
            .on_event(
                0,
                &event!(
                    {
                        "api": {
                            "op": "create", "kind": "binding", "name": "main",
                            "body": {
                                    "/onramp/metronome/1/out": [ "/pipeline/main/1/in" ],
                                    "/pipeline/main/1/out": [ "/offramp/debug/1/in" ],
                            },
                        }
                    }
                ),
            )
            .await;
        assert!(links.is_ok());

        let deploy = api
            .on_event(
                0,
                &event!(
                    {
                        "api": {
                            "op": "activate", "kind": "binding", "name": "main", "instance": "1",
                            "body": {
                                "1": "1"
                            },
                        }
                    }
                ),
            )
            .await;
        assert!(deploy.is_ok());

        Ok(())
    }

    #[async_std::test]
    async fn pubsub_ko_client_cmd() -> Result<()> {
        use crate::temp_network::ws::UrMsg;
        let (uring_tx, _) = bounded::<UrMsg>(1);
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx, uring_tx);
        let mut control = ControlProtocol::new(&conductor);
        let mut sut = PubSubProtocol::new(
            &mut control,
            "pubsub".into(),
            Value::Object(Box::new(hashmap! {})),
        );

        assert_eq!(Ok(()), sut.on_init());

        let actual = sut.on_event(0, &event!({"pubsub": "snot"})).await;
        assert_err!(
            actual,
            "Expect PubSub command value to be a record or `list`"
        );

        let actual = sut.on_event(0, &event!({"pubsub": {}})).await;
        assert_err!(
            actual,
            "PubSub record operations must contain exactly one field/key"
        );

        let actual = sut
            .on_event(0, &event!({"pubsub": { "a": 1, "b": 2}}))
            .await;
        assert_err!(
            actual,
            "PubSub record operations must contain exactly one field/key"
        );

        // Simplest valid pubsub cmd is `list`
        let actual = sut.on_event(0, &event!({"pubsub": "list"})).await;
        assert!(actual.is_ok());

        // Subscribe operation
        let actual = sut
            .on_event(0, &event!({"pubsub": { "subscribe": [ "snot" ]} }))
            .await;
        assert!(actual.is_ok());

        // Unsubscribe
        let actual = sut
            .on_event(0, &event!({"pubsub": { "unsubscribe": [ "snot" ]} }))
            .await;
        assert!(actual.is_ok());

        // Publish
        let actual = sut
            .on_event(0, &event!({"pubsub": { "publish": [ "snot" ]} }))
            .await;
        assert!(actual.is_ok());

        // Unpublish
        let actual = sut
            .on_event(0, &event!({"pubsub": { "unpublish": [ "snot" ]} }))
            .await;
        assert!(actual.is_ok());

        // Send
        let actual = sut
            .on_event(
                0,
                &event!({"pubsub": { "send": { "to": "snot", "data": "fleek" } } }),
            )
            .await;
        assert!(actual.is_ok());

        // List
        let actual = sut.on_event(0, &event!({"pubsub": "list"})).await;
        assert!(actual.is_ok());

        Ok(())
    }

    #[async_std::test]
    async fn pubsub_ok_list_empty() -> Result<()> {
        use crate::temp_network::ws::UrMsg;
        let (uring_tx, _) = bounded::<UrMsg>(1);
        let (tx, _rx) = bounded::<system::ManagerMsg>(1);
        let conductor = Conductor::new(tx, uring_tx);
        let mut control = ControlProtocol::new(&conductor);

        let mut sut = PubSubProtocol::new(
            &mut control,
            "pubsub".into(),
            Value::Object(Box::new(hashmap! {})),
        );

        let actual = sut.on_event(0, &event!({"pubsub": "list"})).await?;
        assert_cont!(actual, { "pubsub": { "list": {
            "subs": [],
            "pubs": [],
        }}});

        Ok(())
    }

    #[async_std::test]
    async fn pubsub_ok_pub_offramp_list() -> Result<()> {
        // FIXME remove. dummy values for testing right now
        let network_endpoint = String::from("127.0.0.1:8140");
        let network_addr: SocketAddr = network_endpoint.parse().unwrap();
        let cluster_endpoint = String::from("127.0.0.1:8139");
        let cluster_peers = vec![];
        let (world, _handle) = World::start(
            64,
            None,
            network_addr,
            cluster_endpoint,
            cluster_peers,
            false,
        )
        .await?;
        let conductor = world.conductor;
        let mut control = ControlProtocol::new(&conductor);
        let mut api = ApiProtocol::new(
            &mut control,
            "api".into(),
            Value::Object(Box::new(hashmap! {})),
        );

        let mut sut = PubSubProtocol::new(
            &mut control,
            "pubsub".into(),
            Value::Object(Box::new(hashmap! {})),
        );

        deploy(&mut api).await?;

        let actual = sut
            .on_event(
                0,
                &event!({"pubsub": { "publish": [ "/offramp/debug/1/in", "/pipeline/main/1/in" ] }}),
            )
            .await;
        assert!(actual.is_ok());
        assert_cont!(actual?, { "pubsub": { "publish-ack": [ "/offramp/debug/1/in", "/pipeline/main/1/in" ]} });

        let actual = sut.on_event(0, &event!({"pubsub": "list"})).await?;
        assert_cont!(actual, { "pubsub": { "list": {
            "pubs": [ "tremor://localhost/offramp/debug/1/in", "tremor://localhost/pipeline/main/1/in" ],
            "subs": [],
        }}});

        let actual = sut
            .on_event(
                0,
                &event!({"pubsub": { "subscribe": [ "/onramp/metronome/1/out", "/pipeline/main/1/out" ] }}),
            )
            .await;
        assert!(actual.is_ok());
        assert_cont!(actual?, { "pubsub": { "subscribe-ack": [ "/onramp/metronome/1/out", "/pipeline/main/1/out"]}});

        let actual = sut.on_event(0, &event!({"pubsub": "list"})).await?;
        assert_cont!(actual, { "pubsub": { "list": {
            "pubs": [ "tremor://localhost/offramp/debug/1/in", "tremor://localhost/pipeline/main/1/in" ],
            "subs": [ "tremor://localhost/onramp/metronome/1/out", "tremor://localhost/pipeline/main/1/out"],
        }}});

        // FIXME Add tests for duplicate subscribe/publish when already subscribed/publishing

        let actual = sut
            .on_event(
                0,
                &event!({"pubsub": { "unsubscribe": [ "/onramp/metronome/1/out", "/pipeline/main/1/out" ] }}),
            )
            .await;
        assert!(actual.is_ok());
        assert_cont!(actual?, { "pubsub": { "unsubscribe-ack": [ "/onramp/metronome/1/out", "/pipeline/main/1/out"]}});

        let actual = sut
            .on_event(
                0,
                &event!({"pubsub": { "unpublish": [ "/offramp/debug/1/in", "/pipeline/main/1/in" ] }}),
            )
            .await;
        assert!(actual.is_ok());
        assert_cont!(actual?, { "pubsub": { "unpublish-ack": [ "/offramp/debug/1/in", "/pipeline/main/1/in" ]} });

        let actual = sut.on_event(0, &event!({"pubsub": "list"})).await?;
        assert_cont!(actual, { "pubsub": { "list": {
            "pubs": [],
            "subs": [],
        }}});

        Ok(())
    }
}
