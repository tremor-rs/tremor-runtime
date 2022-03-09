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

use super::super::visitors::prelude::*;
macro_rules! stop {
    ($e:expr, $leave_fn:expr) => {
        if $e? == VisitRes::Stop {
            return $leave_fn;
        }
    };
}
/// Visitor for traversing all `ImutExprInt`s within the given `ImutExprInt`
///
/// Implement your custom expr visiting logic by overwriting the visit_* methods.
/// You do not need to traverse further down. This is done by the provided `walk_*` methods.
/// The walk_* methods implement walking the expression tree, those do not need to be changed.
pub trait Walker<'script>: QueryWalker<'script> + DeployVisitor<'script> {
    /// walks a `FlowDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_flow_definition(&mut self, defn: &mut FlowDefinition<'script>) -> Result<()> {
        stop!(
            self.visit_flow_definition(defn),
            self.leave_flow_definition(defn)
        );
        self.walk_definitional_args(&mut defn.params)?;
        for create in &mut defn.creates {
            self.walk_create_stmt(create)?;
        }
        for connection in &mut defn.connections {
            self.walk_connect_stmt(connection)?;
        }
        self.leave_flow_definition(defn)
    }

    /// walks a `CreateStmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_create_stmt(&mut self, create: &mut CreateStmt<'script>) -> Result<()> {
        stop!(
            self.visit_create_stmt(create),
            self.leave_create_stmt(create)
        );
        self.walk_creational_with(&mut create.with)?;
        self.walk_create_target_definition(&mut create.defn)?;
        self.leave_create_stmt(create)
    }
    /// walks a `ConnectStmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_connect_stmt(&mut self, connect: &mut ConnectStmt) -> Result<()> {
        stop!(
            self.visit_connect_stmt(connect),
            self.leave_connect_stmt(connect)
        );
        self.walk_deploy_edpoint(connect.from_mut())?;
        self.walk_deploy_edpoint(connect.to_mut())?;

        self.leave_connect_stmt(connect)
    }
    /// walks a `DeployEndpoint`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_deploy_edpoint(&mut self, endpoint: &mut DeployEndpoint) -> Result<()> {
        stop!(
            self.visit_deploy_endpoint(endpoint),
            self.leave_deploy_endpoint(endpoint)
        );
        self.leave_deploy_endpoint(endpoint)
    }
    /// walks a `CreateTargetDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_create_target_definition(
        &mut self,
        defn: &mut CreateTargetDefinition<'script>,
    ) -> Result<()> {
        stop!(
            self.visit_create_target_definition(defn),
            self.leave_create_target_definition(defn)
        );
        match defn {
            CreateTargetDefinition::Connector(c) => self.walk_connector_definition(c)?,
            CreateTargetDefinition::Pipeline(p) => self.walk_pipeline_definition(p)?,
        }
        self.leave_create_target_definition(defn)
    }
    /// walks a `CreateTargetDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn walk_connector_definition(&mut self, defn: &mut ConnectorDefinition<'script>) -> Result<()> {
        stop!(
            self.visit_connector_definition(defn),
            self.leave_connector_definition(defn)
        );
        self.walk_definitinal_args_with(&mut defn.params)?;

        self.leave_connector_definition(defn)
    }
}
