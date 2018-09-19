use elastic;
use reqwest;
use serde_json;
use std::{convert, fmt, num};
/// Generic error
#[derive(Debug, Clone)]
pub struct TSError {
    message: String,
}

impl TSError {
    pub fn new(msg: &str) -> Self {
        TSError {
            message: String::from(msg),
        }
    }
}

impl fmt::Display for TSError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<elastic::Error> for TSError {
    fn from(from: elastic::Error) -> TSError {
        TSError::new(format!("ES Error: {}", from).as_str())
    }
}

impl convert::From<num::ParseFloatError> for TSError {
    fn from(e: num::ParseFloatError) -> TSError {
        TSError::new(&format!("{}", e))
    }
}
impl convert::From<num::ParseIntError> for TSError {
    fn from(e: num::ParseIntError) -> TSError {
        TSError::new(&format!("{}", e))
    }
}

impl From<serde_json::Error> for TSError {
    fn from(e: serde_json::Error) -> TSError {
        TSError::new(format!("Serade error: {}", e).as_str())
    }
}

impl From<reqwest::Error> for TSError {
    fn from(from: reqwest::Error) -> TSError {
        TSError::new(format!("HTTP Error: {}", from).as_str())
    }
}
