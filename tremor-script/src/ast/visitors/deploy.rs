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
use VisitRes::Walk;
/// Visitor for traversing all Parts of a deploy within the given `Deploy`
///
/// Implement your custom expr visiting logic by overwriting the methods.
// #[cfg_attr(coverage, no_coverage)]
pub trait Visitor<'script> {
    /// visit a `FlowDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_flow_definition(&mut self, _with: &mut FlowDefinition<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `FlowDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_flow_definition(&mut self, _with: &mut FlowDefinition<'script>) -> Result<()> {
        Ok(())
    }

    /// visit a `ConnectorDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_connector_definition(
        &mut self,
        _with: &mut ConnectorDefinition<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ConnectorDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_connector_definition(
        &mut self,
        _with: &mut ConnectorDefinition<'script>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `ConnectorDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_create_target_definition(
        &mut self,
        _with: &mut CreateTargetDefinition<'script>,
    ) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `CreateTargetDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_create_target_definition(
        &mut self,
        _with: &mut CreateTargetDefinition<'script>,
    ) -> Result<()> {
        Ok(())
    }

    /// visit a `DeployEndpoint`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_deploy_endpoint(&mut self, _with: &mut DeployEndpoint) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `DeployEndpoint`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_deploy_endpoint(&mut self, _with: &mut DeployEndpoint) -> Result<()> {
        Ok(())
    }
    /// visit a `ConnectStmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_connect_stmt(&mut self, _with: &mut ConnectStmt) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `ConnectStmt`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_connect_stmt(&mut self, _with: &mut ConnectStmt) -> Result<()> {
        Ok(())
    }

    /// visit a `ConnectorDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn visit_create_stmt(&mut self, _with: &mut CreateStmt<'script>) -> Result<VisitRes> {
        Ok(Walk)
    }

    /// leave a `CreateTargetDefinition`
    ///
    /// # Errors
    /// if the walker function fails
    fn leave_create_stmt(&mut self, _with: &mut CreateStmt<'script>) -> Result<()> {
        Ok(())
    }
}
