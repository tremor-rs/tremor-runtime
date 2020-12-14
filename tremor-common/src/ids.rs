// Copyright 2020, The Tremor Team
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

/// we namespace onramp, offramp and operator ids differently in order to avoid clashes
const ONRAMP_ID_BASE: u64 = 0b0;
const OPERATOR_ID_BASE: u64 = 0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_u64;
const OFFRAMP_ID_BASE: u64 = 0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_u64;

#[derive(Debug)]
/// onramp id generator - generates consecutive u64 values
pub struct OnrampIdGen(u64);

impl OnrampIdGen {
    #[must_use]
    /// constructor
    pub fn new() -> Self {
        Self(ONRAMP_ID_BASE)
    }
    /// return the next id for this generator
    pub fn next_id(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        self.0
    }
}

impl Default for OnrampIdGen {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
/// offramp id generator - generates consecutive u64 values
pub struct OfframpIdGen(u64);
impl OfframpIdGen {
    #[must_use]
    /// constructor
    pub fn new() -> Self {
        Self(OFFRAMP_ID_BASE)
    }
    /// return the next id for this generator
    pub fn next_id(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        self.0
    }
}

impl Default for OfframpIdGen {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
/// offramp id generator - generates consecutive u64 values
pub struct OperatorIdGen(u64);
impl OperatorIdGen {
    #[must_use]
    /// constructor
    pub fn new() -> Self {
        Self(OPERATOR_ID_BASE)
    }
    /// return the next id for this generator
    pub fn next_id(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        self.0
    }
}

impl Default for OperatorIdGen {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn onramp_id_gen() {
        let mut idgen = OnrampIdGen::default();
        let ids: Vec<u64> = std::iter::repeat_with(|| idgen.next_id())
            .take(100)
            .collect();

        for window in ids.windows(2) {
            match window {
                &[l, h] => {
                    assert!(l > ONRAMP_ID_BASE);
                    assert!(h > ONRAMP_ID_BASE);
                    assert!(l < h); // strictly monotonically increasing
                }
                _ => assert!(false, "invalid window"),
            }
        }
    }

    #[test]
    fn offramp_id_gen() {
        let mut idgen = OfframpIdGen::default();
        let ids: Vec<u64> = std::iter::repeat_with(|| idgen.next_id())
            .take(100)
            .collect();

        for window in ids.windows(2) {
            match window {
                &[l, h] => {
                    assert!(l > OFFRAMP_ID_BASE);
                    assert!(h > OFFRAMP_ID_BASE);
                    assert!(l < h); // strictly monotonically increasing
                }
                _ => assert!(false, "invalid window"),
            }
        }
    }

    #[test]
    fn operator_id_gen() {
        let mut idgen = OperatorIdGen::default();
        let ids: Vec<u64> = std::iter::repeat_with(|| idgen.next_id())
            .take(100)
            .collect();

        for window in ids.windows(2) {
            match window {
                &[l, h] => {
                    assert!(l > OPERATOR_ID_BASE);
                    assert!(h > OPERATOR_ID_BASE);
                    assert!(l < h); // strictly monotonically increasing
                }
                _ => assert!(false, "invalid window"),
            }
        }
    }
}
