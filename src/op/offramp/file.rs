// Copyright 2018, Wayfair GmbH
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

//! # File Offramp
//!
//! Writes events to a file, one event per line
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::error::TSError;
use crate::errors::*;
use crate::pipeline::prelude::*;
use serde_yaml;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::result;

/// An offramp that write a given file
#[derive(Debug)]
pub struct Offramp {
    file: File,
}

#[derive(Deserialize)]
pub struct Config {
    /// Filename to write to
    pub file: String,
}

impl Offramp {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let file = File::create(config.file)?;
        Ok(Offramp { file })
    }

    fn write_event(&mut self, event: &EventData) -> result::Result<(), TSError> {
        if let EventValue::Raw(ref raw) = event.value {
            self.file.write_all(&raw)?;
            self.file.write_all(b"\n")?;
            Ok(())
        } else {
            Err(TypeError::with_location(&"offramp::file", event.value.t(), ValueType::Raw).into())
        }
    }
}

impl Opable for Offramp {
    // TODO
    fn exec(&mut self, event: EventData) -> EventResult {
        let r = self.write_event(&event);

        match r {
            Ok(_) => EventResult::Return(event.make_return(Ok(None))),
            Err(e) => EventResult::Return(event.make_return(Err(e))),
        }
    }
    fn shutdown(&mut self) {
        let _ = self.file.flush();
    }
    opable_types!(ValueType::Raw, ValueType::Raw);
}

impl From<io::Error> for TSError {
    fn from(from: io::Error) -> TSError {
        TSError::new(&format!("IO Error: {}", from).as_str())
    }
}
