pub mod asy;
pub mod errors;
pub mod file;
pub mod time;

pub use errors::Error;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
