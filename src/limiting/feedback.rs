/// The grouper trait, defining the required functions for a grouper.
pub trait Feedback {
    fn feedback(&mut self, feedback: f64);
}
