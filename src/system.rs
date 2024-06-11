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

/// contains Flow definition, control plane task and lifecycle management
pub mod flow;
/// contains the runtime actor starting and maintaining flows
pub mod flow_supervisor;

use self::flow::Flow;
use crate::errors::{Error, Kind as ErrorKind, Result};
use tokio::{sync::oneshot, task::JoinHandle};
use tremor_connectors::ConnectorBuilder;
use tremor_script::{ast, highlighter::Highlighter};
use tremor_system::{
    killswitch::{KillSwitch, ShutdownMode},
    selector::{PluginType, Rules, RulesBuilder},
};

/// Runtime builder for configuring the runtime
/// note that includees and excludes are handled in order!
/// In other wordds using `with_connector("foo").without_connector("foo`") will result in foo being included
/// this is especially important when using the type based inclusions and exclusions
pub struct RuntimeBuilder {
    connectors: RulesBuilder,
}

impl RuntimeBuilder {
    /// Marks a given connector as includec by name
    #[must_use]
    pub fn with_connector(mut self, connector: &str) -> Self {
        self.connectors = self.connectors.include(connector);
        self
    }
    /// Marks multiple connectors as includec by name
    #[must_use]
    pub fn with_connectors(mut self, connectors: &[&str]) -> Self {
        for connector in connectors {
            self.connectors = self.connectors.include(*connector);
        }
        self
    }
    /// Marks a given connector as excludec by name
    #[must_use]
    pub fn without_connector(mut self, connector: &str) -> Self {
        self.connectors = self.connectors.exclude(connector);
        self
    }
    /// Marks multiple connectors as excludec by name
    #[must_use]
    pub fn without_connectors(mut self, connectors: &[&str]) -> Self {
        for connector in connectors {
            self.connectors = self.connectors.exclude(*connector);
        }
        self
    }

    /// includes debug connectors
    #[must_use]
    pub fn with_debug_connectors(mut self) -> Self {
        self.connectors = self.connectors.include(PluginType::Debug);
        self
    }
    /// excludes debug connectors
    #[must_use]
    pub fn without_debug_connectors(mut self) -> Self {
        self.connectors = self.connectors.exclude(PluginType::Debug);
        self
    }

    /// includes all normal (non debug) connectors
    #[must_use]
    pub fn with_normal_connectors(mut self) -> Self {
        self.connectors = self.connectors.include(PluginType::Normal);
        self
    }
    /// excludes all normal (non debug) connectors
    #[must_use]
    pub fn without_normal_connectors(mut self) -> Self {
        self.connectors = self.connectors.exclude(PluginType::Normal);
        self
    }
    /// If no rule matches, include the connector
    #[must_use]
    pub fn default_include_connectors(self) -> RuntimeConfig {
        let connectors = self.connectors.default_include();
        RuntimeConfig { connectors }
    }

    /// If no rule matches, exclude the connector
    #[must_use]
    pub fn default_exclude_connectors(self) -> RuntimeConfig {
        let connectors = self.connectors.default_exclude();
        RuntimeConfig { connectors }
    }
}

#[derive(Debug, Clone)]
/// Configuration for the runtime
pub struct RuntimeConfig {
    /// if debug connectors should be loaded
    connectors: Rules,
}

impl RuntimeConfig {
    /// Builds the runtime
    /// # Errors
    /// if the runtime can't be started
    pub async fn build(self) -> Result<(Runtime, JoinHandle<Result<()>>)> {
        Runtime::start(self).await
    }
}

/// default timeout for interrogating operations, like listing deployments

/// Tremor runtime
#[derive(Clone, Debug)]
pub struct Runtime {
    pub(crate) system: flow_supervisor::Channel,
    pub(crate) kill_switch: KillSwitch,
    config: RuntimeConfig,
}

impl Runtime {
    /// creates a runtime builder
    #[must_use]
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder {
            connectors: Rules::builder(),
        }
    }
    /// Instantiate a flow from
    /// # Errors
    /// If the flow can't be started
    pub async fn start_flow(&self, flow: &ast::DeployFlow<'static>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.system
            .send(flow_supervisor::Msg::StartDeploy {
                flow: Box::new(flow.clone()),
                sender: tx,
            })
            .await?;
        if let Err(e) = rx.await? {
            let err_str = match e {
                Error(
                    ErrorKind::Script(e)
                    | ErrorKind::Pipeline(tremor_pipeline::errors::ErrorKind::Script(e)),
                    _,
                ) => {
                    let mut h = crate::ToStringHighlighter::new();
                    h.format_error(&tremor_script::errors::Error::from(e))?;
                    h.finalize()?;
                    h.to_string()
                }
                err => err.to_string(),
            };
            error!(
                "Error starting deployment of flow {}: {}",
                flow.instance_alias, &err_str
            );
            Err(ErrorKind::DeployFlowError(flow.instance_alias.clone(), err_str).into())
        } else {
            Ok(())
        }
    }

    /// Registers the given connector type with `type_name` and the corresponding `builder`
    ///
    /// # Errors
    ///  * If the system is unavailable
    pub async fn register_connector(&self, builder: Box<dyn ConnectorBuilder>) -> Result<()> {
        if self
            .config
            .connectors
            .test(&builder.connector_type(), builder.plugin_type())
        {
            self.system
                .send(flow_supervisor::Msg::RegisterConnectorType {
                    connector_type: builder.connector_type(),
                    builder,
                })
                .await?;
        }
        Ok(())
    }

    // METHODS EXPOSED BECAUSE API

    /// Get a flow instance address identified by `flow_id`
    ///
    /// # Errors
    ///  * if we fail to send the request or fail to receive it
    pub async fn get_flow(&self, flow_id: String) -> Result<Flow> {
        let (flow_tx, flow_rx) = oneshot::channel();
        let flow_id = tremor_common::alias::Flow::new(flow_id);
        self.system
            .send(flow_supervisor::Msg::GetFlow(flow_id.clone(), flow_tx))
            .await?;
        flow_rx.await?
    }

    /// list the currently deployed flows
    ///
    /// # Errors
    ///  * if we fail to send the request or fail to receive it
    pub async fn get_flows(&self) -> Result<Vec<Flow>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.system
            .send(flow_supervisor::Msg::GetFlows(reply_tx))
            .await?;
        reply_rx.await?
    }

    /// Starts the runtime system
    ///
    /// # Errors
    ///  * if the world manager can't be started
    async fn start(config: RuntimeConfig) -> Result<(Self, JoinHandle<Result<()>>)> {
        let (system_h, system, kill_switch) = flow_supervisor::FlowSupervisor::new().start();

        let runtime = Self {
            system,
            kill_switch,
            config,
        };

        crate::register_builtin_connector_types(&runtime).await?;
        Ok((runtime, system_h))
    }

    /// Stop the runtime
    ///
    /// # Errors
    ///  * if the system failed to stop
    pub async fn stop(&self, mode: ShutdownMode) -> Result<()> {
        Ok(self.kill_switch.stop(mode).await?)
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_runtime_builder_default_exclude_with() {
        let builder = Runtime::builder();
        let builder = builder.with_connector("foo");
        let builder = builder.with_debug_connectors();
        let builder = builder.with_connectors(&["bar", "baz"]);
        let cfg = builder.default_exclude_connectors();

        assert!(cfg.connectors.test("foo", PluginType::Normal));
        assert!(cfg.connectors.test("foo", PluginType::Debug));

        assert!(cfg.connectors.test("bar", PluginType::Normal));
        assert!(cfg.connectors.test("bar", PluginType::Debug));

        assert!(cfg.connectors.test("baz", PluginType::Normal));

        assert!(cfg.connectors.test("boo", PluginType::Debug));
        assert!(!cfg.connectors.test("boo", PluginType::Normal));
    }

    #[test]
    fn test_runtime_builder_default_include_with() {
        let builder = Runtime::builder();
        let builder = builder.with_connector("foo");
        let builder = builder.without_debug_connectors();
        let builder = builder.with_connectors(&["bar", "baz"]);
        let cfg = builder.default_include_connectors();

        assert!(cfg.connectors.test("foo", PluginType::Normal));
        assert!(cfg.connectors.test("foo", PluginType::Debug));

        assert!(cfg.connectors.test("bar", PluginType::Normal));
        assert!(!cfg.connectors.test("bar", PluginType::Debug));

        assert!(cfg.connectors.test("baz", PluginType::Normal));

        assert!(!cfg.connectors.test("boo", PluginType::Debug));
        assert!(cfg.connectors.test("boo", PluginType::Normal));
    }

    #[test]
    fn test_runtime_builder_default_exclude_without() {
        let builder = Runtime::builder();
        let builder = builder.without_connector("foo");
        let builder = builder.with_normal_connectors();
        let builder = builder.without_connectors(&["bar", "baz"]);
        let cfg = builder.default_exclude_connectors();

        assert!(!cfg.connectors.test("foo", PluginType::Normal));
        assert!(!cfg.connectors.test("foo", PluginType::Debug));

        assert!(cfg.connectors.test("bar", PluginType::Normal));
        assert!(!cfg.connectors.test("bar", PluginType::Debug));

        assert!(cfg.connectors.test("baz", PluginType::Normal));

        assert!(!cfg.connectors.test("boo", PluginType::Debug));
        assert!(cfg.connectors.test("boo", PluginType::Normal));
    }

    #[test]
    fn test_runtime_builder_default_include_without() {
        let builder = Runtime::builder();
        let builder = builder.without_connector("foo");
        let builder = builder.without_normal_connectors();
        let builder = builder.without_connectors(&["bar", "baz"]);
        let cfg = builder.default_include_connectors();

        assert!(!cfg.connectors.test("foo", PluginType::Normal));
        assert!(!cfg.connectors.test("foo", PluginType::Debug));

        assert!(!cfg.connectors.test("bar", PluginType::Normal));
        assert!(!cfg.connectors.test("bar", PluginType::Debug));

        assert!(!cfg.connectors.test("baz", PluginType::Normal));

        assert!(cfg.connectors.test("boo", PluginType::Debug));
        assert!(!cfg.connectors.test("boo", PluginType::Normal));
    }

    #[test]
    fn test_runtime_config() {
        let config = RuntimeConfig {
            connectors: Rules::builder().default_include(),
        };
        let _ = config.build();
    }

    #[tokio::test]
    async fn test_runtime() -> Result<()> {
        let builder = Runtime::builder();
        let config = builder.default_include_connectors();
        let (runtime, _system_h) = Runtime::start(config).await?;
        runtime.stop(ShutdownMode::Graceful).await?;
        Ok(())
    }
}
