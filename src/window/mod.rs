use std::time::Instant;

/// A simple sliding window implementation in rust. It uses a vector as ring
/// buffer and a sum counter to prevent the requirement of iterating over
/// the entire buffer for each time the total count is required.
///
/// This sliding window implementaiton is not aware of the window movement
/// it keeps track of the windows and can be triggered to `tick` over.
#[derive(Debug)]
pub struct SlidingWindow {
    buffer: Vec<u64>,
    sum: u64,
    max: u64,
    pos: usize,
    size: usize,
}

impl SlidingWindow {
    /// Creates a new sliding window.
    ///
    /// * `size` - is the number of slots in the windows
    /// * `max` - is the maximum the current window allows
    pub fn new(size: usize, max: u64) -> Self {
        SlidingWindow {
            buffer: vec![0; size],
            sum: 0,
            max: max,
            pos: 0,
            size: size,
        }
    }

    /// Returns the current count of the window.
    pub fn count(&self) -> u64 {
        self.sum + self.buffer[self.pos]
    }

    /// Tries to increment the window by one, this equals `add(1)`. See `add`
    /// for a description of retrun values.
    pub fn inc(&mut self) -> Result<u64, u64> {
        self.add(1)
    }

    /// Tries to add a given number to the windows count. If we remain below
    /// `max` the result is `Ok(<new count>)`. If we fail and would go over
    /// `max` the result is `Err(<count over max>)`.
    pub fn add(&mut self, n: u64) -> Result<u64, u64> {
        let next = self.sum + self.buffer[self.pos] + n;
        if next <= self.max {
            self.buffer[self.pos] += n;
            Ok(self.buffer[self.pos])
        } else {
            Err(next - self.max)
        }
    }

    //// Moves the window one slot forward.
    pub fn tick(&mut self) -> &mut Self {
        let last = self.buffer[self.pos];
        self.pos = (self.pos + 1) % self.size;
        self.sum = self.sum - self.buffer[self.pos] + last;
        self.buffer[self.pos] = 0;
        self
    }
}

/// A time based window implementation. That will keep track of
/// a total maximum over a given window of time.
#[derive(Debug)]
pub struct TimeWindow {
    window: SlidingWindow,
    slot_time: u64,
    last_tick: Instant,
}

impl TimeWindow {
    /// Creates a new time based window. The arguments are as following:
    ///
    /// * `size` - the number of slots to allocate
    /// * `slot_time` - the time allowed to spend in each slot
    /// * `max` - the maximum allowed over the entire period.
    ///
    /// # Example
    /// to create a window that allows for `1000` entries per second and
    /// keeps track of it at a granularity of `100` ms we would use:
    ///
    /// ```
    /// TimeWindow::new(10, 100, 1000);
    /// ````

    pub fn new(size: usize, slot_time: u64, max: u64) -> Self {
        TimeWindow {
            window: SlidingWindow::new(size, max),
            slot_time: slot_time,
            last_tick: Instant::now(),
        }
    }

    /// Tries to increment the counter by 1. See `SlidingWindow::inc` for details.
    pub fn inc(&mut self) -> Result<u64, u64> {
        self.add(1)
    }
    /// Tries to increment the counter by n. See `SlidingWindow::add` for details.
    pub fn add(&mut self, n: u64) -> Result<u64, u64> {
        let delta = self.last_tick.elapsed();
        let mut delta = (delta.as_secs() * 1_000) + (delta.subsec_nanos() / 1_000_000) as u64;
        if delta > self.slot_time {
            self.last_tick = Instant::now();
            while delta > self.slot_time {
                delta -= self.slot_time;
                self.window.tick();
            }
        }
        self.window.add(n)
    }
    pub fn count(&self) -> u64 {
        self.window.count()
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;
    use window::{SlidingWindow, TimeWindow};

    #[test]
    fn too_much() {
        let mut sw = SlidingWindow::new(10, 10);
        assert!(sw.add(20).is_err())
    }

    #[test]
    fn inc() {
        let mut sw = SlidingWindow::new(10, 10);
        assert_eq!(sw.inc().unwrap(), 1);
        assert_eq!(sw.count(), 1);
    }
    #[test]
    fn all_fields() {
        let mut sw = SlidingWindow::new(10, 10);
        for _i in 0..10 {
            let _ = sw.tick().inc();
        }
        assert_eq!(sw.count(), 10);
    }

    #[test]
    fn rollover() {
        let mut sw = SlidingWindow::new(10, 10);
        for _i in 0..20 {
            let _ = sw.tick().inc();
        }
        assert_eq!(sw.count(), 10);
    }

    #[test]
    fn no_overfill() {
        let mut sw = SlidingWindow::new(10, 10);
        for _i in 0..10 {
            let _ = sw.tick().add(2);
        }
        assert_eq!(sw.count(), 10);
    }

    // This test is rather hidious but since we are testing
    // a time based implementaiton we got to sleep a little.
    //
    // Perhaps some time in the future there is time to
    // investigate a if there is a way to shift time forcefully.

    // We take this out since it's not reliable at the moment
    #[test]
    fn timer() {
        let mut tw = TimeWindow::new(20, 10, 10);
        for _i in 0..10 {
            sleep(Duration::new(0, 21_000_000));
            let _ = tw.inc();
        }
        assert_eq!(tw.count(), 10);
        assert!(tw.inc().is_err());
        println!("{:?}", tw);
        sleep(Duration::new(0, 21_000_000));
        let r = tw.inc();
        println!("{:?}", tw);
        assert!(r.is_ok());
    }

}
