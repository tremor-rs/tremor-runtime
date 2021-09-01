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

use crate::{
    ast::{self, deploy, query},
    errors::{Error, Result},
    prelude::*,
};
use halfbrown::HashMap;
use std::{fmt::Debug, mem, pin::Pin, sync::Arc};

///! This file includes our self referential structs

/// A deployment ( troy ) and it's attached source.
///
/// Implemention analougous to `EventPayload`
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///
#[derive(Clone)]
pub struct Deploy {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    pub(crate) script: ast::Deploy<'static>,
}

#[cfg(not(tarpaulin_include))] // this is a simple Debug implementation
impl Debug for Deploy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.script.fmt(f)
    }
}

/// Captures deployment ready artefacts for one troy unit of deployment
#[derive(Debug)]
pub struct UnitOfDeployment<'script> {
    /// Instances for this deployment unit
    pub instances: HashMap<String, Box<ast::deploy::CreateStmt<'script>>>,
}

impl Deploy {
    /// Provides a Graphviz dot representation of the deployment graph
    #[must_use]
    pub fn dot(&self) -> String {
        self.script.dot()
    }

    /// borrows the script
    #[must_use]
    pub fn suffix(&self) -> &ast::Deploy {
        &self.script
    }
    /// Creates a new Payload with a given byte vector and
    /// a function to turn it into a value and metadata set.
    ///
    /// The return can reference the the data it gets passed
    /// in the function.
    ///
    /// Internally the lifetime will be bound to the raw part
    /// of the struct.
    ///
    /// # Errors
    /// errors if the conversion function fails
    pub fn try_new<E, F>(mut raw: String, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head mut String) -> std::result::Result<ast::Deploy<'head>, E>,
    {
        use ast::Deploy;
        let structured = f(&mut raw)?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured = unsafe { mem::transmute::<Deploy<'_>, Deploy<'static>>(structured) };
        // This is possibl as String::into_bytes just returns the `vec` of the string
        let raw = Pin::new(raw.into_bytes());
        let raw = vec![Arc::new(raw)];
        Ok(Self {
            raw,
            script: structured,
        })
    }

    /// Extracts SRS statements
    ///
    /// This clones all statements
    #[must_use]
    pub fn extract_stmts(&self) -> Vec<DeployStmt> {
        // This is valid since we clone `raw` into each
        // self referential struct, so we keep the data each
        // SRS points to inside the SRS
        self.script
            .stmts
            .iter()
            .cloned()
            .map(|structured| DeployStmt {
                // THIS IS VERY IMPORTANT (a load bearing clone)
                raw: self.raw.clone(),
                structured,
            })
            .collect()
    }

    /// Analyses a deployment file ( troy ) to determine if the
    /// specification is deployable.
    ///
    /// This analysis will check that flow definitions and instances
    /// are correctly defined based on static compile time checks.
    ///
    /// Runtime checks are not performed.
    ///
    /// # Errors
    /// If definitions are incomplete or invalid and instances
    /// are not deployable based on static analysis
    ///
    pub fn as_deployment_unit(&self) -> Result<UnitOfDeployment> {
        use ast::deploy::DeployStmt as StmtKind;
        let mut instances = HashMap::new();

        for stmt in &self.script.stmts {
            match stmt {
                StmtKind::CreateStmt(stmt) => {
                    instances.insert(stmt.id.to_string(), stmt.clone());
                }
                _otherwise => {
                    ();
                }
            }
        }

        Ok(UnitOfDeployment {
            // connectors,
            // pipelines,
            // flows,
            instances,
        })
    }
}

/*
====================================
*/

/// A script and it's attached source.
///
/// Implemention alalougous to `EventPayload`
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///

pub struct Script {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    script: ast::Script<'static>,
}

#[cfg(not(tarpaulin_include))] // this is a simple Debug implementation
impl Debug for Script {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.script.fmt(f)
    }
}

impl Script {
    /// borrows the script
    #[must_use]
    pub fn suffix(&self) -> &ast::Script {
        &self.script
    }
    /// Creates a new Payload with a given byte vector and
    /// a function to turn it into a value and metadata set.
    ///
    /// The return can reference the the data it gets passed
    /// in the function.
    ///
    /// Internally the lifetime will be bound to the raw part
    /// of the struct.
    ///
    /// # Errors
    /// errors if the conversion function fails
    pub fn try_new<E, F>(mut raw: String, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head mut String) -> std::result::Result<ast::Script<'head>, E>,
    {
        use ast::Script;
        let structured = f(&mut raw)?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured = unsafe { mem::transmute::<Script<'_>, Script<'static>>(structured) };
        // This is possibl as String::into_bytes just returns the `vec` of the string
        let raw = Pin::new(raw.into_bytes());
        let raw = vec![Arc::new(raw)];
        Ok(Self {
            raw,
            script: structured,
        })
    }
}

/*
====================================
*/

/// A query and it's attached source.
///
/// Implemention alalougous to `EventPayload`
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///

#[derive(Clone)]
pub struct Query {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    query: ast::Query<'static>,
}

#[cfg(not(tarpaulin_include))] // this is a simple Debug implementation
impl Debug for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.query.fmt(f)
    }
}

impl Query {
    /// Creates a new Query with a pre-existing query
    #[must_use]
    pub fn new_from_ast(structured: ast::Query) -> Self {
        Self {
            raw: vec![Arc::new(Pin::new(vec![]))],
            query: unsafe { mem::transmute(structured) },
        }
    }

    /// borrows the query
    #[must_use]
    pub fn suffix(&self) -> &ast::Query {
        &self.query
    }

    /// Creates a new Query with a given String and
    /// a function to turn it into a value and metadata set.
    ///
    /// The return can reference the the data it gets passed
    /// in the function.
    ///
    /// Internally the lifetime will be bound to the raw part
    /// of the struct.
    ///
    /// # Errors
    /// errors if the conversion function fails
    pub fn try_new<E, F>(mut raw: String, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head mut String) -> std::result::Result<ast::Query<'head>, E>,
    {
        use ast::Query;
        let structured = f(&mut raw)?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured = unsafe { mem::transmute::<Query<'_>, Query<'static>>(structured) };
        // This is possibl as String::into_bytes just returns the `vec` of the string
        let raw = Pin::new(raw.into_bytes());
        let raw = vec![Arc::new(raw)];
        Ok(Self {
            raw,
            query: structured,
        })
    }

    /// Extracts SRS statements
    ///
    /// This clones all statements
    #[must_use]
    pub fn extract_stmts(&self) -> Vec<Stmt> {
        // This is valid since we clone `raw` into each
        // self referential struct, so we keep the data each
        // SRS points to inside the SRS
        self.query
            .stmts
            .iter()
            .cloned()
            .map(|structured| Stmt {
                // THIS IS VERY IMPORTANT (a load bearing clone)
                raw: self.raw.clone(),
                structured,
            })
            .collect()
    }
}

/*
====================================
*/

/// A troy tatement and it's attached source.
///
/// Implemention analougous to `EventPayload`
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///
#[derive(Clone)]
pub struct DeployStmt {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    structured: ast::deploy::DeployStmt<'static>,
}

#[cfg(not(tarpaulin_include))] // this is a simple Debug implementation
impl Debug for DeployStmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.structured.fmt(f)
    }
}

mod eq_for_deploy_stmt {
    ///! We have this simply for the same of allowing `NodeConfig` to be `PartialEq` for the use in
    ///! and `PartialOrd`.
    ///!
    ///! We define equality and order by the metadata Id's as they identify statements
    ///! so two code wise equal statements that are re-typed won't be considered equal
    use crate::ast::BaseExpr;

    use super::DeployStmt;
    impl PartialEq for DeployStmt {
        fn eq(&self, other: &Self) -> bool {
            self.structured.mid() == other.structured.mid()
        }
    }

    /// We order statements by their mid
    impl PartialOrd for DeployStmt {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.structured.mid().partial_cmp(&other.structured.mid())
        }
    }
    impl Eq for DeployStmt {}
}

impl DeployStmt {
    /// borrow the suffix
    #[must_use]
    pub fn suffix(&self) -> &ast::DeployStmt {
        &self.structured
    }
    /// Creates a new statement from another SRS
    ///
    /// # Errors
    /// if query `f` errors
    pub fn try_new_from_query<E, F>(other: &Deploy, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(
            &'head ast::Deploy,
        ) -> std::result::Result<ast::deploy::DeployStmt<'head>, E>,
    {
        use ast::deploy::DeployStmt;
        let raw = other.raw.clone();
        let structured = f(other.suffix())?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured =
            unsafe { mem::transmute::<DeployStmt<'_>, DeployStmt<'static>>(structured) };

        Ok(Self { raw, structured })
    }
}

/*
====================================
*/

/// A statement and it's attached source.
///
/// Implemention analougous to `EventPayload`
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///
#[derive(Clone)]
pub struct Stmt {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    structured: ast::Stmt<'static>,
}

#[cfg(not(tarpaulin_include))] // this is a simple Debug implementation
impl Debug for Stmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.structured.fmt(f)
    }
}

mod eq_for_stmt {
    ///! We have this simply for the same of allowing `NodeConfig` to be `PartialEq` for the use in
    ///! and `PartialOrd`.
    ///!
    ///! We define equality and order by the metadata Id's as they identify statements
    ///! so two code wise equal statements that are re-typed won't be considered equal
    use crate::ast::BaseExpr;

    use super::Stmt;
    impl PartialEq for Stmt {
        fn eq(&self, other: &Self) -> bool {
            self.structured.mid() == other.structured.mid()
        }
    }

    /// We order statements by their mid
    impl PartialOrd for Stmt {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.structured.mid().partial_cmp(&other.structured.mid())
        }
    }
    impl Eq for Stmt {}
}

impl Stmt {
    /// borrow the suffix
    #[must_use]
    pub fn suffix(&self) -> &ast::Stmt {
        &self.structured
    }
    /// Creates a new statement from another SRS
    ///
    /// # Errors
    /// if query `f` errors
    pub fn try_new_from_query<E, F>(other: &Query, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head ast::Query) -> std::result::Result<ast::Stmt<'head>, E>,
    {
        use ast::Stmt;
        let raw = other.raw.clone();
        let structured = f(other.suffix())?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured = unsafe { mem::transmute::<Stmt<'_>, Stmt<'static>>(structured) };

        Ok(Self { raw, structured })
    }
}

/*
=========================================================================
*/

/// A query declaration
#[derive(Clone)]
pub struct PipelineDecl {
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    script: ast::PipelineDecl<'static>,
}

#[cfg(not(tarpaulin_include))] // this is a simple Debug implementation
impl Debug for PipelineDecl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.script.fmt(f)
    }
}

impl PipelineDecl {
    /// Access to the raw part of the script
    #[must_use]
    pub fn raw(&self) -> &[Arc<Pin<Vec<u8>>>] {
        &self.raw
    }

    /// Creates a new decl from a statement
    ///
    /// # Errors
    /// if decl isn't a script declaration
    pub fn try_new_from_stmt(decl: &DeployStmt) -> Result<Self> {
        let raw = decl.raw.clone();

        let script = match &decl.structured {
            deploy::DeployStmt::PipelineDecl(script) => *script.clone(),
            _other => return Err("Trying to turn a non query into a query operator".into()),
        };

        // FIXME - add support for const params to query
        // script.query.consts.args = Value::object();
        // if let Some(p) = &script.params {
        //     // Set params from decl as meta vars
        //     for (name, value) in p {
        //         // We could clone here since we bind Script to defn_rentwrapped.stmt's lifetime
        //         script
        //             .query
        //             .consts
        //             .args
        //             .try_insert(name.clone(), value.clone());
        //     }
        // }

        Ok(Self { raw, script })
    }

    /// Applies a statment to the decl
    ///
    /// # Errors
    /// if stmt is ot a Script
    pub fn apply_stmt(&mut self, stmt: &DeployStmt) -> Result<()> {
        // We append first in the case that some data already moved into self.structured by the time
        // that the join_f fails
        self.raw.extend_from_slice(&stmt.raw);

        if let ast::deploy::DeployStmt::PipelineDecl(_instance) = &stmt.structured {
            // FIXME add support for params
            // if let Some(map) = &instance.params {
            //     for (name, value) in map {
            //         // We can not clone here since we do not bind Script to node_rentwrapped's lifetime
            //         self.script
            //             .scripts
            //             .consts
            //             .args
            //             .try_insert(name.clone(), value.clone());
            //     }
            // }
            Ok(())
        } else {
            Err("Trying to turn something into script create that isn't a script create".into())
        }
    }
}

/*
=========================================================================
*/

/// A script declaration
#[derive(Clone)]
pub struct ScriptDecl {
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    script: ast::ScriptDecl<'static>,
}

#[cfg(not(tarpaulin_include))] // this is a simple Debug implementation
impl Debug for ScriptDecl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.script.fmt(f)
    }
}

impl ScriptDecl {
    /// Access to the raw part of the script
    #[must_use]
    pub fn raw(&self) -> &[Arc<Pin<Vec<u8>>>] {
        &self.raw
    }
    /// Creates a new decl from a statement
    ///
    /// # Errors
    /// if decl isn't a script declaration
    pub fn try_new_from_stmt(decl: &Stmt) -> Result<Self> {
        let raw = decl.raw.clone();

        let mut script = match &decl.structured {
            query::Stmt::ScriptDecl(script) => *script.clone(),
            _other => return Err("Trying to turn a non script into a script operator".into()),
        };
        script.script.consts.args = Value::object();

        if let Some(p) = &script.params {
            // Set params from decl as meta vars
            for (name, value) in p {
                // We could clone here since we bind Script to defn_rentwrapped.stmt's lifetime
                script
                    .script
                    .consts
                    .args
                    .try_insert(name.clone(), value.clone());
            }
        }

        Ok(Self { raw, script })
    }

    /// Applies a statment to the decl
    ///
    /// # Errors
    /// if stmt is ot a Script
    pub fn apply_stmt(&mut self, stmt: &Stmt) -> Result<()> {
        // We append first in the case that some data already moved into self.structured by the time
        // that the join_f fails
        self.raw.extend_from_slice(&stmt.raw);

        if let query::Stmt::Script(instance) = &stmt.structured {
            if let Some(map) = &instance.params {
                for (name, value) in map {
                    // We can not clone here since we do not bind Script to node_rentwrapped's lifetime
                    self.script
                        .script
                        .consts
                        .args
                        .try_insert(name.clone(), value.clone());
                }
            }
            Ok(())
        } else {
            Err("Trying to turn something into script create that isn't a script create".into())
        }
    }
}

/*
=========================================================================
*/

/// A select statement
#[derive(Clone)]
pub struct Select {
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    select: ast::SelectStmt<'static>,
}

#[cfg(not(tarpaulin_include))] // this is a simple Debug implementation
impl Debug for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.select.fmt(f)
    }
}

impl Select {
    /// Named after the original rental struct for easy rewriting.
    ///
    /// Borrows the borrowed (liftimed) part of the self referential struct
    /// and calls the provided function with a reference to it
    pub fn rent<F, R>(&self, f: F) -> R
    where
        F: for<'iref, 'head> FnOnce(&'iref ast::SelectStmt<'head>) -> R,
        R: ,
    {
        f(&self.select)
    }

    /// Named after the original rental struct for easy rewriting.
    ///
    /// Borrows the borrowed (liftimed) part of the self referential struct
    /// mutably and calls the provided function with a mutatable reference to it
    pub fn rent_mut<F, R>(&mut self, f: F) -> R
    where
        F: for<'iref, 'head> FnOnce(&'iref mut ast::SelectStmt<'head>) -> R,
        R: ,
    {
        f(&mut self.select)
    }

    /// Tries to create a new select from a statement
    ///
    /// # Errors
    /// if other isn't a select statment
    pub fn try_new_from_stmt(other: &Stmt) -> Result<Self> {
        use ast::SelectStmt as Select;
        if let ast::Stmt::Select(select) = other.suffix() {
            let raw = other.raw.clone();
            // This is where the magic happens
            // ALLOW: this is sound since we implement a self referential struct
            let select = unsafe { mem::transmute::<Select<'_>, Select<'static>>(select.clone()) };
            Ok(Self { raw, select })
        } else {
            Err(Error::from(
                "Trying to turn a non select into a select operator",
            ))
        }
    }
}

/*
=========================================================================
*/

/// A event payload in form of two borrowed Value's with a vector of source binaries.
///
/// We have a vector to hold multiple raw input values
///   - Each input value is a Vec<u8>
///   - Each Vec is pinned to ensure the underlying data isn't moved
///   - Each Pin is in a Arc so we can clone the data without with both clones
///     still pointing to the underlying pin.
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///
#[derive(Clone, Default)]
pub struct EventPayload {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    data: ValueAndMeta<'static>,
}

#[cfg(not(tarpaulin_include))] // this is a simple Debug implementation
impl Debug for EventPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.data.fmt(f)
    }
}

impl EventPayload {
    /// Gets the suffix
    ///
    /// ```compile_fail
    ///   use tremor_script::prelude::*;
    ///   let vec = br#"{"key": "value"}"#.to_vec();
    ///   let e = EventPayload::new(vec, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let v: Value = {
    ///       let s = e.suffix();
    ///       s.value()["key"].clone()
    ///   };
    ///   drop(e);
    ///   println!("v: {}", v)
    /// ```

    #[must_use]
    pub fn suffix(&self) -> &ValueAndMeta {
        &self.data
    }

    /// a function to turn it into a value and metadata set.
    ///
    /// The return can reference the the data it gets passed
    /// in the function.
    ///
    /// Internally the lifetime will be bound to the raw part
    /// of the struct.
    #[must_use]
    pub fn new<F>(raw: Vec<u8>, f: F) -> Self
    where
        F: for<'head> FnOnce(&'head mut [u8]) -> ValueAndMeta<'head>,
    {
        let mut raw = Pin::new(raw);
        let data = f(raw.as_mut().get_mut());
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured = unsafe { mem::transmute::<ValueAndMeta<'_>, ValueAndMeta<'static>>(data) };
        let raw = vec![Arc::new(raw)];
        Self {
            raw,
            data: structured,
        }
    }

    /// Creates a new Payload with a given byte vector and
    /// a function to turn it into a value and metadata set.
    ///
    /// The return can reference the the data it gets passed
    /// in the function.
    ///
    /// Internally the lifetime will be bound to the raw part
    /// of the struct.
    ///
    /// # Errors
    /// errors if the conversion function fails
    pub fn try_new<E, F>(raw: Vec<u8>, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head mut [u8]) -> std::result::Result<ValueAndMeta<'head>, E>,
    {
        let mut raw = Pin::new(raw);
        let data = f(raw.as_mut().get_mut())?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured = unsafe { mem::transmute::<ValueAndMeta<'_>, ValueAndMeta<'static>>(data) };
        let raw = vec![Arc::new(raw)];
        Ok(Self {
            raw,
            data: structured,
        })
    }

    /// Named after the original rental struct for easy rewriting.
    ///
    /// Borrows the borrowed (liftimed) part of the self referential struct
    /// and calls the provided function with a reference to it
    ///
    /// ```compile_fail
    ///   use tremor_script::prelude::*;
    ///   let vec = br#"{"key": "value"}"#.to_vec();
    ///   let e = EventPayload::new(vec, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let v: Value = e.rent(|s| {
    ///       s.value()["key"].clone()
    ///   });
    ///   println!("v: {}", v)
    /// ```
    pub fn rent<'iref, F, R>(&'iref self, f: F) -> R
    where
        F: for<'head> FnOnce(&'head ValueAndMeta<'head>) -> R,
    {
        // we are turning a longer lifetime into a shorter one for a covariant
        // type ValueAndMeta. &mut is invariant over it's lifetime, but we are
        // choosing a shorter one and passing it down not up. So a user
        // should not be able to choose an inappropriate lifetime for it, plus
        // they don't control the owner here.
        f(unsafe {
            // ALLOW: See above explenation
            mem::transmute::<&'iref ValueAndMeta<'static>, &'iref ValueAndMeta<'iref>>(&self.data)
        })
    }

    /// Named after the original rental struct for easy rewriting.
    ///
    /// Borrows the borrowed (liftimed) part of the self referential struct
    /// mutably and calls the provided function with a mutatable reference to it
    /// ```compile_fail
    ///   use tremor_script::prelude::*;
    ///   let vec = br#"{"key": "value"}"#.to_vec();
    ///   let mut e = EventPayload::new(vec, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let v: Value = e.rent_mut(|s| {
    ///       s.value()["key"].clone()
    ///   });
    ///   println!("v: {}", v)
    /// ```
    pub fn rent_mut<'iref, F, R>(&'iref mut self, f: F) -> R
    where
        F: for<'head> FnOnce(&'head mut ValueAndMeta<'head>) -> R,
    {
        // we are turning a longer lifetime into a shorter one for a covariant
        // type ValueAndMeta. &mut is invariant over it's lifetime, but we are
        // choosing a shorter one and passing it down not up. So a user should
        // not be able to choose an inappropriate lifetime for it, plus they
        // don't control the owner here.
        f(unsafe {
            // ALLOW: See above explenation
            mem::transmute::<&'iref mut ValueAndMeta<'static>, &'iref mut ValueAndMeta<'iref>>(
                &mut self.data,
            )
        })
    }

    /// Borrow the parts (event and metadata) from a rental.
    /// ```compile_fail
    ///   use tremor_script::prelude::*;
    ///   let vec = br#"{"key": "value"}"#.to_vec();
    ///   let e = EventPayload::new(vec, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let v: Value = {
    ///       let (s, _) = e.parts();
    ///       s["key"].clone()
    ///   };
    ///   drop(e);
    ///   println!("v: {}", v)
    /// ```
    #[must_use]
    pub fn parts<'value, 'borrow>(&'borrow self) -> (&'borrow Value<'value>, &'borrow Value<'value>)
    where
        'borrow: 'value,
    {
        let ValueAndMeta { ref v, ref m } = self.data;
        (v, m)
    }

    /// Consumes one payload into another
    ///
    /// ```compile_fail
    ///   use tremor_script::prelude::*;
    ///   use tremor_script::errors::Error;
    ///   let vec1 = br#"{"key": "value"}"#.to_vec();
    ///   let mut e1 = EventPayload::new(vec1, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let vec2 = br#"{"snot": "badger"}"#.to_vec();
    ///   let e2 = EventPayload::new(vec2, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let mut v = Value::null();
    ///   // We try to move the data ov v2 outside of this closure to trick the borrow checker
    ///   // into letting us have it even if it's referenced data no longer exist
    ///   e1.consume::<Error,_>(e2, |v1, v2| {
    ///     let (v2,_) = v2.into_parts();
    ///     v = v2;
    ///     Ok(())
    ///   }).unwrap();
    ///   drop(e1);
    ///   println!("v: {}", v);
    /// ```
    ///
    /// # Errors
    /// if `join_f` errors
    pub fn consume<'iref, E, F>(
        &'iref mut self,
        mut other: EventPayload,
        join_f: F,
    ) -> std::result::Result<(), E>
    where
        E: std::error::Error,
        F: for<'head> FnOnce(
            &'head mut ValueAndMeta<'head>,
            ValueAndMeta<'head>,
        ) -> std::result::Result<(), E>,
    {
        // We append first in the case that some data already moved into self.structured by the time
        // that the join_f fails
        // READ: ORDER MATTERS!
        self.raw.append(&mut other.raw);

        //  we are turning a longer lifetime into a shorter one for a covariant
        // type ValueAndMeta. &mut is invariant over it's lifetime, but we are
        // choosing a shorter one and passing it down not up. So a user should
        // not be able to choose an inappropriate lifetime for it, plus they
        // don't control the owner here.
        join_f(
            unsafe {
                // ALLOW: See above for explenation
                mem::transmute::<&'iref mut ValueAndMeta<'static>, &'iref mut ValueAndMeta<'iref>>(
                    &mut self.data,
                )
            },
            // ALLOW: See above for explenation
            unsafe { mem::transmute::<ValueAndMeta<'static>, ValueAndMeta<'iref>>(other.data) },
        )
    }

    /// Applies another SRS into this, this functions **needs** to
    ///
    /// # Errors
    /// if `join_f` errors
    pub fn apply_decl<R, F>(&mut self, other: &ScriptDecl, apply_f: F) -> R
    where
        F: for<'iref, 'head> FnOnce(
            &'iref mut ValueAndMeta<'head>,
            &'iref ast::ScriptDecl<'head>,
        ) -> R,
        R: ,
    {
        // We append first in the case that some data already moved into self.structured by the time
        // that the join_f fails
        // READ: ORDER MATTERS!
        self.raw.extend_from_slice(other.raw());

        // We can access `other.script` here with it's static lifetime since we did clone the `raw`
        // into our own `raw` before. This equalizes `iref` and `head` for `self` and `other`
        apply_f(&mut self.data, &other.script)
    }

    /// Applies another SRS into this, this functions **needs** to
    ///
    /// # Errors
    /// if `join_f` errors
    pub fn apply_script<R, F>(&mut self, other: &Script, apply_f: F) -> R
    where
        F: for<'iref, 'head> FnOnce(&'iref mut ValueAndMeta<'head>, &'iref ast::Script<'head>) -> R,
        R: ,
    {
        // We append first in the case that some data already moved into self.structured by the time
        // that the join_f fails
        // READ: ORDER MATTERS!
        self.raw.extend_from_slice(&other.raw);

        // We can access `other.script` here with it's static lifetime since we did clone the `raw`
        // into our own `raw` before. This equalizes `iref` and `head` for `self` and `other`
        apply_f(&mut self.data, &other.script)
    }

    /// Applies another SRS into this, this functions **needs** to
    ///
    /// # Errors
    /// if `join_f` errors
    pub fn apply_select<R, F>(&mut self, other: &mut Select, apply_f: F) -> R
    where
        F: for<'iref, 'head> FnOnce(
            &'iref mut ValueAndMeta<'head>,
            &'iref mut ast::SelectStmt<'head>,
        ) -> R,
        R: ,
    {
        // We append first in the case that some data already moved into self.structured by the time
        // that the join_f fails
        // READ: ORDER MATTERS!
        self.raw.extend_from_slice(&other.raw);

        // We can access `other.script` here with it's static lifetime since we did clone the `raw`
        // into our own `raw` before. This equalizes `iref` and `head` for `self` and `other`
        apply_f(&mut self.data, &mut other.select)
    }
}

impl<T> From<T> for EventPayload
where
    ValueAndMeta<'static>: From<T>,
{
    fn from(vm: T) -> Self {
        Self {
            raw: Vec::new(),
            data: vm.into(),
        }
    }
}

impl PartialEq for EventPayload {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}

impl simd_json_derive::Serialize for EventPayload {
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        self.rent(|d| d.json_write(writer))
    }
}

impl<'input> simd_json_derive::Deserialize<'input> for EventPayload {
    fn from_tape(tape: &mut simd_json_derive::Tape<'input>) -> simd_json::Result<Self>
    where
        Self: Sized + 'input,
    {
        let ValueAndMeta { v, m } = simd_json_derive::Deserialize::from_tape(tape)?;

        Ok(Self::new(vec![], |_| {
            ValueAndMeta::from_parts(v.into_static(), m.into_static())
        }))
    }
}

/*
=========================================================================
*/

/// Combined struct for an event value and metadata
#[derive(
    Clone, Debug, PartialEq, Serialize, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct ValueAndMeta<'event> {
    v: Value<'event>,
    m: Value<'event>,
}

impl<'event> ValueAndMeta<'event> {
    /// A value from it's parts
    #[must_use]
    pub fn from_parts(v: Value<'event>, m: Value<'event>) -> Self {
        Self { v, m }
    }
    /// Event value
    #[must_use]
    pub fn value(&self) -> &Value<'event> {
        &self.v
    }

    /// Event value
    #[must_use]
    pub fn value_mut(&mut self) -> &mut Value<'event> {
        &mut self.v
    }
    /// Event metadata
    #[must_use]
    pub fn meta(&self) -> &Value<'event> {
        &self.m
    }
    /// Deconstruicts the value into it's parts
    #[must_use]
    pub fn into_parts(self) -> (Value<'event>, Value<'event>) {
        (self.v, self.m)
    }
    /// borrows both parts as mutalbe
    #[must_use]
    pub fn parts_mut(&mut self) -> (&mut Value<'event>, &mut Value<'event>) {
        (&mut self.v, &mut self.m)
    }

    /// borrows both parts as mutalbe
    #[must_use]
    pub fn parts(&self) -> (&Value<'event>, &Value<'event>) {
        (&self.v, &self.m)
    }
}

impl<'event> Default for ValueAndMeta<'event> {
    fn default() -> Self {
        ValueAndMeta {
            v: Value::object(),
            m: Value::object(),
        }
    }
}

impl<'v> From<Value<'v>> for ValueAndMeta<'v> {
    fn from(v: Value<'v>) -> ValueAndMeta<'v> {
        ValueAndMeta {
            v,
            m: Value::object(),
        }
    }
}

impl<'v, T1, T2> From<(T1, T2)> for ValueAndMeta<'v>
where
    Value<'v>: From<T1> + From<T2>,
{
    fn from((v, m): (T1, T2)) -> Self {
        ValueAndMeta {
            v: Value::from(v),
            m: Value::from(m),
        }
    }
}
