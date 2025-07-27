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

/// Our own Result convenience type
pub type Result<T> = std::result::Result<T, Error>;

/// Tremor Pipeline / Trickle Error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Tremor-script Errors
    #[error(transparent)]
    Script(#[from] tremor_script::errors::Error),
    /// Yaml Errors
    #[error("Error during YAML parsing: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// JSON Errors
    #[error(transparent)]
    Json(#[from] simd_json::Error),
    /// Invalid access into JSON document
    #[error(transparent)]
    JsonAccess(#[from] value_trait::AccessError),
    /// Invalid URL
    #[error(transparent)]
    UrlParser(#[from] url::ParseError),
    /// IO Error
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Error converting binary data to UTF8 String
    #[error(transparent)]
    FromUtf8String(#[from] std::string::FromUtf8Error),
    /// Error converting binary data to UTF8 str
    #[error(transparent)]
    FromUtf8Str(#[from] std::str::Utf8Error),
    /// Invalid int string
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
    /// Invalid float string
    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),
    /// Rust Lock/Mutex poison error
    #[error("Poison Error")]
    Poison,
    /// Regular Expression Error
    #[error(transparent)]
    Regex(#[from] regex::Error),
    /// Bert Error
    #[cfg(feature = "bert")]
    #[error(transparent)]
    Bert(#[from] rust_bert::RustBertError),
    /// Sled Error
    #[error(transparent)]
    Sled(#[from] sled::Error),
    /// Error handling a sled transaction
    #[error("Sled Transaction Error: {0:?}")]
    SledTransaction(String),
    /// Tremor Value Error
    #[error(transparent)]
    TremorValue(#[from] tremor_value::Error),
    /// A generic str based error
    #[error("{0}")]
    Msg(&'static str),
    #[cfg(test)]
    /// Testing-only String-based error
    #[error("{0}")]
    String(String),
    /*
     * Query language pipeline conversion errors
     */
    /// Generic trickle error
    #[error("Error detected in trickle: {0}")]
    PipelineError(String),
    /// the given pipeline would create a cyclic graph
    #[error("Cycle detected in graph: {0}")]
    CyclicGraphError(String),
    /// Missing operator config
    #[error("Missing Operator config: {0}")]
    MissingOpConfig(String),
    /// Provided operator config for an operator that don't need no configuration
    #[error("Operator {0} has a config but can't be configured")]
    ExtraOpConfig(String),
    /// Invalid operator configuration
    #[error("Operator config has bad syntax: {0}")]
    BadOpConfig(String),
    /// Unknown operator
    #[error("Unknown operator: {0}::{1}")]
    UnknownOp(String, String),
    /// Unknown namespace
    #[error("Unknown namespace: {0}")]
    UnknownNamespace(String),
    /// Invalid input stream name
    #[error("Invalid input stream name '{}' for pipeline '{}'.", .stream_name, .pipeline)]
    InvalidInputStreamName {
        /// Invalid stream name
        stream_name: String,
        /// context pipeline
        pipeline: String,
    },
    /// Pipeline graph node not found
    #[error("Invalid pipeline: Node {0} not found")]
    NodeNotFound(usize),
    /// Pipeline graph is invalid
    #[error("Invalid pipeline graph: {0}")]
    InvalidGraph(Box<GraphError>),
    /// Pipeline sub graph is invalid
    #[error("Invalid Sub-graph: {0}")]
    InvalidSubGraph(Box<SubGraphError>),
}

#[cfg(test)]
impl From<String> for Error {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}
impl From<&'static str> for Error {
    fn from(value: &'static str) -> Self {
        Self::Msg(value)
    }
}

impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(_: std::sync::PoisonError<P>) -> Self {
        Self::Poison
    }
}

impl<P: std::fmt::Debug> From<sled::transaction::TransactionError<P>> for Error {
    fn from(value: sled::transaction::TransactionError<P>) -> Self {
        Self::SledTransaction(format!("{value:?}"))
    }
}

/// An error in a pipeline subgraph - most likely an internal tremor issue
#[derive(Debug, thiserror::Error)]
pub enum SubGraphError {
    /// Bad output port
    #[error("bad output port {}, availabile: {}", .port, .available.join(", "))]
    BadOutputPort {
        /// output port
        port: String,
        /// available ports
        available: Vec<String>,
    },
    /// Bad input port
    #[error("bad input port {}, availabile: {}", .port, .available.join(", "))]
    BadInputPort {
        /// input ports
        port: String,
        /// available ports
        available: Vec<String>,
    },
}

impl From<SubGraphError> for Error {
    fn from(value: SubGraphError) -> Self {
        Self::InvalidSubGraph(Box::new(value))
    }
}

/// An error in a pipeline graph - most likely an internal tremor issue
#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    /// Invalid edge
    #[error("Invalid edge with idx: {0}")]
    InvalidEdge(usize),
    /// Invalid from node
    #[error("Invalid from graph node with idx: {0}")]
    InvalidFromNode(usize),
    /// Invalid to node
    #[error("Invalid to graph node with idx: {0}")]
    InvalidToNode(usize),
    /// Invalid pipeline input
    #[error("Invalid pipeline input {0}")]
    InvalidInput(String),
    /// Invalid pipeline output
    #[error("Invalid pipeline output {0}")]
    InvalidOutput(String),
}

impl From<GraphError> for Error {
    fn from(value: GraphError) -> Self {
        Self::InvalidGraph(Box::new(value))
    }
}

/// Creates a missing config field error
#[must_use]
pub fn missing_config(f: &str) -> Error {
    Error::MissingOpConfig(format!("missing field {f}"))
}
