// Copyright 2022-2024, The Tremor Team
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

use crate::{controlplane, dataplane, dataplane::contraflow, event::Event};
use simd_json::OwnedValue;
use std::{
    collections::BTreeMap,
    fmt::{self, Display},
    str::FromStr,
};
use tokio::sync::mpsc::{error::SendError, Sender, UnboundedSender};
use tremor_common::{alias, ids::OperatorId};

/// Address for a pipeline
#[derive(Clone)]
pub struct Addr {
    addr: Sender<Box<dataplane::Msg>>,
    cf_addr: UnboundedSender<contraflow::Msg>,
    mgmt_addr: Sender<controlplane::Msg>,
    alias: alias::Pipeline,
}

impl Addr {
    /// creates a new address
    #[must_use]
    pub fn new(
        addr: Sender<Box<dataplane::Msg>>,
        cf_addr: UnboundedSender<contraflow::Msg>,
        mgmt_addr: Sender<controlplane::Msg>,
        alias: alias::Pipeline,
    ) -> Self {
        Self {
            addr,
            cf_addr,
            mgmt_addr,
            alias,
        }
    }

    /// send a contraflow insight message back down the pipeline
    ///
    /// # Errors
    /// * if sending failed
    pub fn send_insight(&self, event: Event) -> Result<(), SendError<contraflow::Msg>> {
        self.cf_addr.send(contraflow::Msg::Insight(event))
    }

    /// send a data-plane message to the pipeline
    ///
    /// # Errors
    /// * if sending failed
    pub async fn send(
        &self,
        msg: Box<dataplane::Msg>,
    ) -> Result<(), SendError<Box<dataplane::Msg>>> {
        self.addr.send(msg).await
    }

    /// send a control-plane message to the pipeline
    ///
    /// # Errors
    /// * if sending failed
    pub async fn send_mgmt(
        &self,
        msg: controlplane::Msg,
    ) -> Result<(), SendError<controlplane::Msg>> {
        self.mgmt_addr.send(msg).await
    }

    /// stop the pipeline
    ///     
    /// # Errors
    /// * if sending failed
    pub async fn stop(&self) -> Result<(), SendError<controlplane::Msg>> {
        self.send_mgmt(controlplane::Msg::Stop).await
    }

    /// start the pipeline
    ///     
    /// # Errors
    /// * if sending failed
    pub async fn start(&self) -> Result<(), SendError<controlplane::Msg>> {
        self.send_mgmt(controlplane::Msg::Start).await
    }

    /// pause the pipeline
    ///
    /// # Errors
    /// * if sending failed
    pub async fn pause(&self) -> Result<(), SendError<controlplane::Msg>> {
        self.send_mgmt(controlplane::Msg::Pause).await
    }

    /// resume the pipeline
    ///     
    /// # Errors
    /// * if sending failed
    pub async fn resume(&self) -> Result<(), SendError<controlplane::Msg>> {
        self.send_mgmt(controlplane::Msg::Resume).await
    }
}

impl fmt::Debug for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pipeline({})", self.alias)
    }
}

/// Stringified numeric key
/// from <https://github.com/serde-rs/json-benchmark/blob/master/src/prim_str.rs>
#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct PrimStr<T>(pub T)
where
    T: Copy + Ord + Display + FromStr;

impl<T> simd_json_derive::SerializeAsKey for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        write!(writer, "\"{}\"", self.0)
    }
}

impl<T> simd_json_derive::Serialize for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        write!(writer, "\"{}\"", self.0)
    }
}

impl<'input, T> simd_json_derive::Deserialize<'input> for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    #[inline]
    fn from_tape(tape: &mut simd_json_derive::Tape<'input>) -> simd_json::Result<Self>
    where
        Self: std::marker::Sized + 'input,
    {
        if let Some(simd_json::Node::String(s)) = tape.next() {
            Ok(PrimStr(FromStr::from_str(s).map_err(|_e| {
                simd_json::Error::generic(simd_json::ErrorType::Serde("not a number".into()))
            })?))
        } else {
            Err(simd_json::Error::generic(
                simd_json::ErrorType::ExpectedNull,
            ))
        }
    }
}

/// Operator metadata
#[derive(
    Clone, Debug, Default, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
// TODO: optimization: - use two Vecs, one for operator ids, one for operator metadata (Values)
//                     - make it possible to trace operators with and without metadata
//                     - insert with bisect (numbers of operators tracked will be low single digit numbers most of the time)
pub struct OpMeta(BTreeMap<PrimStr<OperatorId>, OwnedValue>);

impl OpMeta {
    /// inserts a value
    pub fn insert<V>(&mut self, key: OperatorId, value: V) -> Option<OwnedValue>
    where
        OwnedValue: From<V>,
    {
        self.0.insert(PrimStr(key), OwnedValue::from(value))
    }
    /// reads a value
    pub fn get(&mut self, key: OperatorId) -> Option<&OwnedValue> {
        self.0.get(&PrimStr(key))
    }
    /// checks existance of a key
    #[must_use]
    pub fn contains_key(&self, key: OperatorId) -> bool {
        self.0.contains_key(&PrimStr(key))
    }

    /// Merges two op meta maps, overwriting values with `other` on duplicates
    pub fn merge(&mut self, mut other: Self) {
        self.0.append(&mut other.0);
    }

    /// Returns `true` if this instance contains no values
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Pipeline status report
pub mod report {
    use std::collections::HashMap;

    use crate::{
        dataplane::{InputTarget, OutputTarget},
        instance::State,
    };
    use tremor_common::ports::Port;
    use tremor_script::ast::DeployEndpoint;

    /// Status report for a pipeline
    #[derive(Debug, Clone)]
    pub struct Status {
        pub(crate) state: State,
        pub(crate) inputs: Vec<Input>,
        pub(crate) outputs: HashMap<String, Vec<Output>>,
    }

    impl Status {
        /// create a new status report
        #[must_use]

        pub fn new(
            state: State,
            inputs: Vec<Input>,
            outputs: HashMap<String, Vec<Output>>,
        ) -> Self {
            Self {
                state,
                inputs,
                outputs,
            }
        }
        /// state of the pipeline   
        #[must_use]
        pub fn state(&self) -> State {
            self.state
        }
        /// inputs of the pipeline
        #[must_use]
        pub fn inputs(&self) -> &[Input] {
            &self.inputs
        }
        /// outputs of the pipeline
        #[must_use]
        pub fn outputs(&self) -> &HashMap<String, Vec<Output>> {
            &self.outputs
        }

        /// outputs of the pipeline
        #[must_use]
        pub fn outputs_mut(&mut self) -> &mut HashMap<String, Vec<Output>> {
            &mut self.outputs
        }
    }

    /// Input report
    #[derive(Debug, Clone, PartialEq)]
    pub enum Input {
        /// Pipeline input report
        Pipeline {
            /// alias of the pipeline
            alias: String,

            /// port of the pipeline
            port: Port<'static>,
        },
        /// Source input report
        Source {
            /// alias of the source
            alias: String,
            /// port of the source
            port: Port<'static>,
        },
    }

    impl Input {
        /// create a new pipeline input report
        #[must_use]
        pub fn pipeline(alias: &str, port: Port<'static>) -> Self {
            Self::Pipeline {
                alias: alias.to_string(),
                port,
            }
        }
        /// create a new source input report
        #[must_use]
        pub fn source(alias: &str, port: Port<'static>) -> Self {
            Self::Source {
                alias: alias.to_string(),
                port,
            }
        }
        /// create a new input report
        #[must_use]
        pub fn new(endpoint: &DeployEndpoint, target: &InputTarget) -> Self {
            match target {
                InputTarget::Pipeline(_addr) => {
                    Input::pipeline(endpoint.alias(), endpoint.port().clone())
                }
                InputTarget::Source(_addr) => {
                    Input::source(endpoint.alias(), endpoint.port().clone())
                }
            }
        }
    }

    /// Output report
    #[derive(Debug, Clone, PartialEq)]
    pub enum Output {
        /// Pipeline output report
        Pipeline {
            /// alias of the pipeline
            alias: String,
            /// port of the pipeline
            port: Port<'static>,
        },
        /// Sink output report
        Sink {
            /// alias of the sink
            alias: String,
            /// port of the sink
            port: Port<'static>,
        },
    }

    impl Output {
        /// create a new pipeline output report
        #[must_use]
        pub fn pipeline(alias: &str, port: Port<'static>) -> Self {
            Self::Pipeline {
                alias: alias.to_string(),
                port,
            }
        }
        /// create a new sink output report
        #[must_use]
        pub fn sink(alias: &str, port: Port<'static>) -> Self {
            Self::Sink {
                alias: alias.to_string(),
                port,
            }
        }
    }
    impl From<&(DeployEndpoint, OutputTarget)> for Output {
        fn from(target: &(DeployEndpoint, OutputTarget)) -> Self {
            match target {
                (endpoint, OutputTarget::Pipeline(_)) => {
                    Output::pipeline(endpoint.alias(), endpoint.port().clone())
                }
                (endpoint, OutputTarget::Sink(_)) => {
                    Output::sink(endpoint.alias(), endpoint.port().clone())
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::OpMeta;
    use simd_json::prelude::ValueAsScalar;
    use tremor_common::ids::{Id, OperatorId};

    #[test]
    fn op_meta_merge() {
        let op_id1 = OperatorId::new(1);
        let op_id2 = OperatorId::new(2);
        let op_id3 = OperatorId::new(3);
        let mut m1 = OpMeta::default();
        let mut m2 = OpMeta::default();
        m1.insert(op_id1, 1);
        m1.insert(op_id2, 1);
        m2.insert(op_id1, 2);
        m2.insert(op_id3, 2);
        m1.merge(m2);

        assert!(m1.contains_key(op_id1));
        assert!(m1.contains_key(op_id2));
        assert!(m1.contains_key(op_id3));

        assert_eq!(m1.get(op_id1).as_u64(), Some(2));
        assert_eq!(m1.get(op_id2).as_u64(), Some(1));
        assert_eq!(m1.get(op_id3).as_u64(), Some(2));
    }
}
