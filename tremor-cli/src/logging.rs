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

use crate::errors::Result;
use log::LevelFilter;
use log4rs::{
    config::{Appender, Root},
    Config,
};
use tremor_pipeline::{PluggableLoggingAppender, LOGGING_CHANNEL};

pub fn run() -> Result<()> {
    let tx = LOGGING_CHANNEL.tx();

    let stdout = PluggableLoggingAppender { tx };

    let config = match Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Debug))
    {
        Ok(config) => config,
        Err(_) => return Err("pluggable logging config could not be built".into()),
    };

    match log4rs::init_config(config) {
        Ok(_) => Ok(()),
        Err(err) => Err(err.to_string().into()),
    }
}
