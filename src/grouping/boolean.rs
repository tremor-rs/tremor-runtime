//! A simple grouper that ignores the classification and either passes or drops a message.

use classifier::Classified;
use error::TSError;
use grouping::utils::{Grouper as GrouperT, MaybeMessage};

/// A grouper either drops or keeps all messages.
pub struct Grouper {
    drop: bool,
}
impl Grouper {
    /// Creates a new boolean grouper with either drop set to either `"true"` or `"false"`
    pub fn new(opts: &str) -> Self {
        match opts {
            "true" => Grouper { drop: true },
            "false" => Grouper { drop: false },
            _ => panic!("Unknown option for Boolean grouper, valid options are true and false."),
        }
    }
}
impl GrouperT for Grouper {
    fn group<'c, 'p>(&mut self, msg: Classified<'c, 'p>) -> Result<MaybeMessage<'p>, TSError> {
        Ok(MaybeMessage {
            drop: self.drop,
            msg: msg.msg,
        })
    }
}
