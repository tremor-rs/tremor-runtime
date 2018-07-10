/// Generic error
#[derive(Debug)]
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
