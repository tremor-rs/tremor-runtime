use elastic;
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

impl From<elastic::Error> for TSError {
    fn from(from: elastic::Error) -> TSError {
        TSError::new(format!("ES Error: {}", from).as_str())
    }
}
