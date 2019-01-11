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

//! # Periodic time source Onramp
//!
//! An onramp that periodically injects a tick message based on a given config.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Example
//! ```yaml
//! - onramp::metronome:
//!   period: 1000 # # 1000 millis, or 1 second, must be at least 1ms
//!   pipelines:
//!     - floop
//!     - fleep
//! ```

use crate::errors::*;
use crate::onramp::{EnterReturn, Onramp as OnrampT, PipelineOnramp, PipelineOnrampElem};
use crate::pipeline::prelude::*;
use crate::utils::{nanotime, park};

use serde_json;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

lazy_static! {
    static ref HUN_NS: Duration = Duration::from_nanos(100);
}

pub struct Onramp {
    config: Config,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    // Period in milliseconds for interval between tick events
    period: u64,
}

impl Onramp {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;

        if config.period == 0 {
            return Err(ErrorKind::BadOpConfig(format!(
                "Configured metronome period must be a non-zero positive integer ({})",
                config.period
            ))
            .into());
        }

        Ok(Onramp {
            config: config.clone(),
        })
    }
}

pub fn tick(next: u64, period: u64, pipelines: &[PipelineOnrampElem]) -> u64 {
    let mut now = nanotime();

    while next > now {
        park(*HUN_NS);
        now = nanotime();
    }

    // println!("next: {} vs now: {}", next, now);

    for p in pipelines {
        p.do_send(OnData {
            reply_channel: None,
            data: EventValue::JSON(json!({
                "metronome": "periodic",
                "at": now,
            })),
            vars: HashMap::new(),
            ingest_ns: now,
        });
    }

    next + period
}

impl OnrampT for Onramp {
    fn enter_loop(&mut self, pipelines: PipelineOnramp) -> EnterReturn {
        let period = self.config.period * 1_000_000;

        thread::spawn(move || loop {
            let mut next = nanotime() + period;
            loop {
                next = tick(next, period, &pipelines);
            }
        })

        // TODO FIXME When ramp registry is complete, we need a shutdownAll
        //    that can be used to break infinite loop's on shutdown. Currently
        //    only the 'special' blaster onramp benefits but without this change
        //    pipelines with metronomes are *NOT* blastable
        //
        //     for p in pipelines {
        //         p.send(Shutdown).wait();
        //     }
        // })
    }
}
