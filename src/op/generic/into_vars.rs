//!
//! # Tremor into_var Operation
//!
//! Sets a variable to the value from a key of a document.
//!
//!
//! ## Config
//!
//! * `var` - name of the variable to set
//! * `key` - the key to get the variable from
//! * `required` - boolean, if set to true the event will be send to the error output if the key is not in the document. (default: false)
//!
//! ## Output Variables
//!
//! * `<var>`  if required is set to true

use error::TSError;
use errors::*;
use pipeline::prelude::*;
use serde_json;
use std::fmt;

fn d_false() -> bool {
    false
}
#[derive(Deserialize)]
pub struct Op {
    var: String,
    keys: Vec<String>,
    #[serde(default = "d_false")]
    required: bool,
}
impl fmt::Debug for Op {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IntoVar(key: '{:?}', variable: '{}')",
            self.keys, self.var
        )
    }
}

impl Op {
    pub fn new(opts: &ConfValue) -> Result<Self> {
        let o: Self = serde_yaml::from_value(opts.clone())?;
        Ok(o)
    }
}

impl Opable for Op {
    fn exec(&mut self, mut event: EventData) -> EventResult {
        if !event.is_type(ValueType::JSON) {
            let t = event.value.t();
            return EventResult::Error(
                event,
                Some(TSError::from(TypeError::with_location(
                    &"op::into_var2",
                    t,
                    ValueType::JSON,
                ))),
            );
        };

        let val: Option<MetaValue> = {
            if let EventValue::JSON(ref val) = event.value {
                let ks = &self.keys;
                let vs: Vec<String> = ks
                    .iter()
                    .filter_map(|k| match val.get(&k) {
                        Some(serde_json::Value::String(s)) => Some(s.clone()),
                        _ => None,
                    }).collect();

                if vs.len() == ks.len() {
                    Some(MetaValue::VecS(vs))
                } else {
                    None
                }
            } else {
                unreachable!()
            }
        };

        match val {
            Some(val) => {
                event.set_var(&self.var, val);
                EventResult::Next(event)
            }
            None => if self.required {
                EventResult::Error(event, Some(TSError::new(&format!("Key `{:?}` needs to be present but was not. So the variable `{}` can not be set.", self.keys, self.var))))
            } else {
                EventResult::Next(event)
            },
        }
    }

    fn output_vars(&self) -> HashSet<String> {
        let mut h = HashSet::new();
        if self.required {
            h.insert(self.var.clone());
        };
        h
    }

    opable_types!(ValueType::JSON, ValueType::JSON);
}
