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

//! Collection of common unique numeric identifiers for internal use.
//! Those are more efficient than stupid strings.

use std::{marker::PhantomData, ops::Deref};

/// operator uid
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
pub struct OperatorUId(u64);

impl UId for OperatorUId {
    fn new(id: u64) -> Self {
        Self(id)
    }

    fn id(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for OperatorUId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for OperatorUId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let p: u64 = s.parse()?;
        Ok(Self::new(p))
    }
}

/// connector uid
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
pub struct ConnectorUId(u64);
impl UId for ConnectorUId {
    fn new(id: u64) -> Self {
        Self(id)
    }

    fn id(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ConnectorUId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for ConnectorUId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<u64> for ConnectorUId {
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
pub struct SinkUId(ConnectorUId);
impl From<ConnectorUId> for SinkUId {
    fn from(cid: ConnectorUId) -> Self {
        Self(cid)
    }
}

impl UId for SinkUId {
    fn new(id: u64) -> Self {
        Self(ConnectorUId::new(id))
    }

    fn id(&self) -> u64 {
        self.0.id()
    }
}

impl std::fmt::Display for SinkUId {
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
pub struct SourceUId(ConnectorUId);

impl UId for SourceUId {
    fn new(id: u64) -> Self {
        Self(ConnectorUId::new(id))
    }

    fn id(&self) -> u64 {
        self.0.id()
    }
}

impl From<ConnectorUId> for SourceUId {
    fn from(cid: ConnectorUId) -> Self {
        Self(cid)
    }
}
impl std::fmt::Display for SourceUId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Source({})", self.0)
    }
}

/// Unique numeric Identifier trait used everywhere within tremor
pub trait UId {
    /// constructor from a unique integer
    fn new(id: u64) -> Self;

    /// get the id value
    fn id(&self) -> u64;
}

#[derive(Debug)]
/// id generator
pub struct UIdGen<T: UId> {
    current: u64,
    _marker: PhantomData<T>,
}

impl<T: UId> UIdGen<T> {
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

impl<T: UId> Default for UIdGen<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// operator id generator - generates consecutive u64 values
/// this one will be shared by all pipelines - so we ensure unique operator ids
pub type OperatorUIdGen = UIdGen<OperatorUId>;
/// connector id generator - generates consecutive u64 values
pub type ConnectorUIdGen = UIdGen<ConnectorUId>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn id_gen() {
        let mut idgen = UIdGen::<ConnectorUId>::default();
        let ids: Vec<ConnectorUId> = std::iter::repeat_with(|| idgen.next_id())
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
        let id = OperatorUId::from_str("1024");

        assert_eq!(Ok(OperatorUId(1024)), id);
    }
}
