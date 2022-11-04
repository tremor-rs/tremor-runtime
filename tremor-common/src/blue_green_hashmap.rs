// Copyright 2022, The Tremor Team
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

use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, SystemTime};

/// The `BlueGreenHashmap` is a `HashMap`, where all items live for a specified duration (and never less)
///
/// This is achieved by internally keeping two `HashMap`s, each with a validity start time (from which
/// the end of validity can be computed).
/// When the map is initialised, one of the hashmaps is marked as already expired, and the second
/// one is used for writes.
/// When an insert happens it checks which `HashMap` is expired and it writes to the one that isn't.
/// When both `HashMap`s expire, the older one is dropped (and replaced with a new empty one, which
/// will contain new items), which means the elements will have lived for a time in range
/// (now-expiration*2, now-expiration], therefore satisfying the minimum expiration time.
///
/// Also known as flip-flop data mop
#[allow(unused)]
pub struct BlueGreenHashMap<K: Send, V: Send> {
    expiration: Duration,
    hashmap_blue: (HashMap<K, V>, SystemTime),
    hashmap_green: (HashMap<K, V>, SystemTime),
}

impl<K, V> BlueGreenHashMap<K, V>
where
    K: Eq + Hash + Send,
    V: Send,
{
    /// Create a new instance. Items contained will leave for at least `expiration`.
    /// `now` stands for current time, other methods take is an argument to do time-sensitive calculations
    #[must_use]
    pub fn new(expiration: Duration, now: SystemTime) -> Self {
        Self {
            expiration,
            hashmap_blue: (HashMap::new(), now),
            hashmap_green: (HashMap::new(), now - expiration),
        }
    }

    /// insert a `value` at `key`
    pub fn insert(&mut self, key: K, value: V, now: SystemTime) {
        self.get_unexpired_hashmap(now).insert(key, value);
    }

    /// remove the value at `key`
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(x) = self.hashmap_blue.0.remove(key) {
            Some(x)
        } else {
            self.hashmap_green.0.remove(key)
        }
    }

    fn get_unexpired_hashmap(&mut self, now: SystemTime) -> &mut HashMap<K, V> {
        let blue_creation_time = self.hashmap_blue.1;
        let green_creation_time = self.hashmap_green.1;

        if blue_creation_time + self.expiration > now && blue_creation_time < green_creation_time {
            return &mut self.hashmap_blue.0;
        } else if green_creation_time + self.expiration > now {
            return &mut self.hashmap_green.0;
        }

        if green_creation_time > blue_creation_time {
            self.hashmap_blue = (HashMap::new(), now);

            &mut self.hashmap_blue.0
        } else {
            self.hashmap_green = (HashMap::new(), now);

            &mut self.hashmap_green.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn can_access_after_writing() {
        let mut hashmap = BlueGreenHashMap::new(Duration::from_secs(10), SystemTime::now());
        hashmap.insert("a", 1234, SystemTime::now());

        assert_eq!(Some(1234), hashmap.remove(&"a"));
    }

    #[test]
    pub fn removes_expired_entries() {
        let start_time = SystemTime::now();
        let mut hashmap = BlueGreenHashMap::new(Duration::from_secs(10), start_time);
        hashmap.insert("a".to_string(), "b", start_time);

        // GC is performed on insertion of the next element
        hashmap.insert("b".to_string(), "c", start_time + Duration::from_secs(20));

        assert_eq!(None, hashmap.remove(&"a".to_string()));
        assert_eq!(Some("c"), hashmap.remove(&"b".to_string()));
    }
}
