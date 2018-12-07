use crate::error::TSError;
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
    fn exec(&mut self, event: EventData) -> EventResult {
        ensure_type!(event, "runtime::php", ValueType::Raw);

        let res = event.replace_value(|val| {
            if let EventValue::Raw(raw) = val {
                let ctx = IOContext {
                    body: raw.clone().into_boxed_slice(),
                    buffer: Vec::with_capacity(1028),
                };
                if let Ok(result) = self.runtime.execute(&self.config.file, ctx) {
                    Ok(EventValue::Raw(result.buffer.to_vec()))
                } else {
                    Err(TSError::new(&"Failed to execute php"))
                }
            } else {
                unreachable!()
            }
        });
        match res {
            Ok(n) => EventResult::Next(n),
            Err(e) => e,
        }
    }
}
