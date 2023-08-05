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

// common id handling

use std::{marker::PhantomData, ops::Deref};

/// operator id
#[derive(
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Clone,
    Copy,
    Default,
    simd_json_derive::Serialize,
    simd_json_derive::Deserialize,
)]
pub struct OperatorId(u64);

impl Id for OperatorId {
    fn new(id: u64) -> Self {
        Self(id)
    }

    fn id(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for OperatorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for OperatorId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let p: u64 = s.parse()?;
        Ok(Self::new(p))
    }
}

/// connector id
#[derive(
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Clone,
    Copy,
    Default,
    simd_json_derive::Serialize,
    simd_json_derive::Deserialize,
)]
pub struct ConnectorId(u64);
impl Id for ConnectorId {
    fn new(id: u64) -> Self {
        Self(id)
    }

    fn id(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ConnectorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for ConnectorId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<u64> for ConnectorId {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

/// Sink Identifier (reuses connector id of the containing connector)
#[derive(
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Clone,
    Copy,
    Default,
    simd_json_derive::Serialize,
    simd_json_derive::Deserialize,
)]
pub struct SinkId(ConnectorId);
impl From<ConnectorId> for SinkId {
    fn from(cid: ConnectorId) -> Self {
        Self(cid)
    }
}

impl Id for SinkId {
    fn new(id: u64) -> Self {
        Self(ConnectorId::new(id))
    }

    fn id(&self) -> u64 {
        self.0.id()
    }
}

impl std::fmt::Display for SinkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sink({})", self.0)
    }
}

/// Source Identifier (reuses connector id of the containing connector)
#[derive(
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Clone,
    Copy,
    Default,
    simd_json_derive::Serialize,
    simd_json_derive::Deserialize,
)]
pub struct SourceId(ConnectorId);

impl Id for SourceId {
    fn new(id: u64) -> Self {
        Self(ConnectorId::new(id))
    }

    fn id(&self) -> u64 {
        self.0.id()
    }
}

impl From<ConnectorId> for SourceId {
    fn from(cid: ConnectorId) -> Self {
        Self(cid)
    }
}
impl std::fmt::Display for SourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Source({})", self.0)
    }
}

/// Identifier trait used everywhere within tremor
pub trait Id {
    /// constructor from a unique integer
    fn new(id: u64) -> Self;

    /// get the id value
    fn id(&self) -> u64;
}

#[derive(Debug)]
/// id generator
pub struct IdGen<T: Id> {
    current: u64,
    _marker: PhantomData<T>,
}

impl<T: Id> IdGen<T> {
    #[must_use]
    /// constructor
    pub fn new() -> Self {
        Self {
            current: 0,
            _marker: PhantomData,
        }
    }
    /// return the next id for this generator
    pub fn next_id(&mut self) -> T {
        self.current = self.current.wrapping_add(1);
        T::new(self.current)
    }
}

impl<T: Id> Default for IdGen<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// operator id generator - generates consecutive u64 values
/// this one will be shared by all pipelines - so we ensure unique operator ids
pub type OperatorIdGen = IdGen<OperatorId>;
/// connector id generator - generates consecutive u64 values
pub type ConnectorIdGen = IdGen<ConnectorId>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn id_gen() {
        let mut idgen = IdGen::<ConnectorId>::default();
        let ids: Vec<ConnectorId> = std::iter::repeat_with(|| idgen.next_id())
            .take(100)
            .collect();

        for window in ids.windows(2) {
            match window {
                &[l, h] => {
                    assert!(l < h); // strictly monotonically increasing
                }
                _ => panic!("invalid window"),
            }
        }
    }

    #[test]
    fn id_can_be_created_from_string() {
        let id = OperatorId::from_str("1024");

        assert_eq!(Ok(OperatorId(1024)), id);
    }
}
