use std::fmt::Display;

#[derive(Debug)]
pub enum Error {
    FileOpen(std::io::Error, String),
}

impl Display for Error {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FileOpen(e, f) => write!(w, "Failed to open file `{}`: {}", f, e),
        }
    }
}

impl std::error::Error for Error {}
