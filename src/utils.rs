// Copyright 2018-2019, Wayfair GmbH
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

// FIXME: make util crate! .unwrap()

use crate::errors::*;
use chrono::{Timelike, Utc};
use regex::Regex;
use serde_yaml::Value;
use std::time::Duration;

pub fn duration_to_millis(at: Duration) -> u64 {
    (at.as_secs() as u64 * 1_000) + (u64::from(at.subsec_nanos()) / 1_000_000)
}

pub fn nanotime() -> u64 {
    let now = Utc::now();
    let seconds: u64 = now.timestamp() as u64;
    let nanoseconds: u64 = u64::from(now.nanosecond());

    (seconds * 1_000_000_000) + nanoseconds
}

pub trait ConfigImpl {
    fn new(config: &Value) -> Result<Self>
    where
        for<'de> Self: serde::de::Deserialize<'de>,
    {
        // simpler ways, but does not give us the kind of error info we want
        //let validated_config: Config = serde_yaml::from_value(c.clone())?;
        //let validated_config: Config = serde_yaml::from_str(&serde_yaml::to_string(c)?)?;

        // serialize the YAML config and deserialize it again, so that we get extra info on
        // YAML errors here (eg: name of the config key where the errror occured). can just
        // use serde_yaml::from_value() here, but the error message there is limited.
        match serde_yaml::from_str(&serde_yaml::to_string(config)?) {
            Ok(c) => Ok(c),
            Err(e) => {
                // remove the potentially misleading "at line..." info, since it does not
                // correspond to the numbers in the file now
                let re = Regex::new(r" at line \d+ column \d+$")?;
                let e_cleaned = re.replace(&e.to_string(), "").into_owned();
                Err(e_cleaned.into())
            }
        }
    }
}
