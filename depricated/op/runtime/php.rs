// Copyright 2018-2020, Wayfair GmbH
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

use crate::errors::*;
use crate::pipeline::prelude::*;
use php::{IOContext, Runtime as PHPRuntime};
use serde_yaml;
use std::fmt;

#[derive(Deserialize)]
struct Config {
    file: String,
}

pub struct Runtime {
    config: Config,
    runtime: PHPRuntime<IOContext>,
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "php-runtime: {}", self.config.file)
    }
}

impl Runtime {
    pub fn create(opts: &ConfValue) -> Result<Runtime> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let runtime =
            IOContext::add_to_builder(PHPRuntime::new("tremor-php", "Tremor PHP runtime", 1))
                .start();
        Ok(Self { config, runtime })
    }
}

impl Opable for Runtime {
    opable_types!(ValueType::Raw, ValueType::Raw);
    fn on_event(&mut self, event: EventData) -> EventResult {
        ensure_type!(event, "runtime::php", ValueType::Raw);
        let res = event.replace_value(|val| {
            if let EventValue::Raw(raw) = val {
                let mut ctx = IOContext {
                    body: raw.clone().into_boxed_slice(),
                    buffer: Vec::with_capacity(1028),
                };
                self.runtime
                    .execute(&self.config.file, &mut ctx)
                    .map_err(|_| Error::from("PHP execution error"))?;
                Ok(EventValue::Raw(ctx.buffer.to_vec()))
            } else {
                unreachable!()
            }
        });
        match res {
            Ok(n) => next!(n),
            Err(e) => e,
        }
    }
}
