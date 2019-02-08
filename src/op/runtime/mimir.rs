use crate::errors::*;
use crate::pipeline::prelude::*;
use mimir::Script;

#[derive(Deserialize, Debug)]
struct Config {
    script: String,
}

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
    fn on_event(&mut self, event: EventData) -> EventResult {
        ensure_type!(event, "runtime::mimir", ValueType::JSON);
        let mut event = event;
        let res = match (&mut event.value, &mut event.vars) {
            (EventValue::JSON(ref mut json), ref mut meta) => self.runtime.run(json, meta),
            _ => unreachable!(),
        };

        match res {
            Ok(_) => next!(event),
            Err(e) => EventResult::Error(Box::new(event), Some(e.into())),
        }
    }
}
