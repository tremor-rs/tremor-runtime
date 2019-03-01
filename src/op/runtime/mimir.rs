use crate::errors::*;
use crate::pipeline::prelude::*;
use mimir::{registry::Context, Script};

#[derive(Deserialize, Debug)]
struct Config {
    script: String,
}

#[derive(Debug)]
struct TremorContext {
    ingest_ns: u64,
}

impl Context for TremorContext {}

#[derive(Debug)]
pub struct Runtime {
    config: Config,
    runtime: Script,
}

impl Runtime {
    pub fn create(opts: &ConfValue) -> Result<Runtime> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let runtime = Script::parse(&config.script)?;
        Ok(Self { config, runtime })
    }
}

impl Opable for Runtime {
    opable_types!(ValueType::JSON, ValueType::JSON);
    fn on_event(&mut self, mut event: EventData) -> EventResult {
        ensure_type!(event, "runtime::mimir", ValueType::JSON);
        let context = TremorContext {
            ingest_ns: event.ingest_ns,
        };

        match event.value {
            EventValue::JSON(ref mut json) => {
                match self.runtime.run(&context, json, &mut event.vars) {
                    Ok(_) => next!(event),
                    Err(e) => EventResult::Error(Box::new(event), Some(e.into())),
                }
            }
            _ => unreachable!(),
        }
    }
}
