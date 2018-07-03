use error::TSError;
use errors::*;
use pipeline::prelude::*;
use serde_json;

#[derive(Debug)]
pub struct Parser {}
impl Parser {
    pub fn new(_opts: &ConfValue) -> Result<Self> {
        Ok(Parser {})
    }
}

impl Opable for Parser {
    fn exec(&mut self, event: EventData) -> EventResult {
        if !event.is_type(ValueType::Raw) {
            let t = event.value.t();
            return EventResult::Error(
                event,
                Some(TSError::from(TypeError::with_location(
                    &"parse::json",
                    t,
                    ValueType::Raw,
                ))),
            );
        };
        let res = event.replace_value(|val| {
            if let EventValue::Raw(raw) = val {
                match serde_json::from_slice(raw) {
                    Err(_) => Err(TSError::new(&"Bad JSON")),
                    Ok(doc) => Ok(EventValue::JSON(doc)),
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
    opable_types!(ValueType::Raw, ValueType::JSON);
}

#[derive(Debug)]
pub struct Renderer {}
impl Renderer {
    pub fn new(_opts: &ConfValue) -> Result<Self> {
        Ok(Self {})
    }
}

impl Opable for Renderer {
    fn exec(&mut self, event: EventData) -> EventResult {
        if !event.is_type(ValueType::JSON) {
            let t = event.value.t();
            return EventResult::Error(
                event,
                Some(TSError::from(TypeError::with_location(
                    &"render::json",
                    t,
                    ValueType::JSON,
                ))),
            );
        };
        let res = event.replace_value(|val| {
            if let EventValue::JSON(ref val) = val {
                if let Ok(json) = serde_json::to_vec(val) {
                    Ok(EventValue::Raw(json))
                } else {
                    Err(TSError::new(&"Bad JSON"))
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
    opable_types!(ValueType::JSON, ValueType::Raw);
}
