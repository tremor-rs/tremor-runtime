// Copyright 2018-2019, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![forbid(warnings)]
#![recursion_limit = "1024"]
#![cfg_attr(
    feature = "cargo-clippy",
    deny(
        clippy::all,
        clippy::result_unwrap_used,
        clippy::unnecessary_unwrap,
        clippy::pedantic
    )
)]

#[cfg(test)]
#[macro_use]
extern crate proptest;
extern crate chrono;
use chrono::Timelike;
use chrono::Utc;

/// A simple sliding window implementation in rust. It uses a vector as ring
/// buffer and a sum counter to prevent the requirement of iterating over
/// the entire buffer for each time the total count is required.
///
/// This sliding window implementation is not aware of the window movement
/// it keeps track of the windows and can be triggered to `tick` over.
#[derive(Debug)]
pub struct SlidingWindow<T> {
    buffer: Vec<T>,
    sum: T,
    max: T,
    pos: usize,
    size: usize,
}

impl<T> SlidingWindow<T>
where
    T: std::clone::Clone
        + std::marker::Copy
        + std::ops::Add<Output = T>
        + std::ops::Sub<Output = T>
        + std::convert::From<u32>
        + std::cmp::PartialOrd,
{
    /// Creates a new sliding window.
    ///
    /// * `size` - is the number of slots in the windows
    /// * `max` - is the maximum the current window allows
    pub fn new(size: usize, max: T) -> Self {
        Self {
            buffer: vec![T::from(0); size],
            sum: T::from(0),
            max,
            pos: 0,
            size,
        }
    }

    pub fn max(&self) -> T {
        self.max
    }
    pub fn set_max(&mut self, max: T) -> &Self {
        self.max = max;
        self
    }
    /// Returns the current count of the window.
    pub fn count(&self) -> T {
        self.sum + self.buffer[self.pos]
    }

    /// Tries to increment the window by one, this equals `add(1)`. See `add`
    /// for a description of return values.
    pub fn inc(&mut self) -> Result<T, T> {
        self.add(T::from(1))
    }

    /// Tries to add a given number to the windows count. If we remain below
    /// `max` the result is `Ok(<new count>)`. If we fail and would go over
    /// `max` the result is `Err(<count over max>)`.
    pub fn add(&mut self, n: T) -> Result<T, T> {
        let next = self.sum + self.buffer[self.pos] + n;
        if next <= self.max {
            self.buffer[self.pos] = self.buffer[self.pos] + n;
            Ok(self.buffer[self.pos])
        } else {
            Err(next - self.max)
        }
    }

    pub fn empty(&mut self) -> &mut Self {
        self.buffer = vec![T::from(0); self.size];
        self.sum = T::from(0);
        self.pos = 0;
        self
    }

    //// Moves the window one slot forward.
    pub fn tick(&mut self) -> &mut Self {
        let last = self.buffer[self.pos];
        self.pos = (self.pos + 1) % self.size;
        self.sum = self.sum - self.buffer[self.pos] + last;
        self.buffer[self.pos] = T::from(0);
        self
    }
}

/// A time based window implementation. That will keep track of
/// a total maximum over a given window of time.
#[derive(Debug)]
pub struct TimeWindow {
    window: SlidingWindow<u64>,
    slot_time: u64,
    last_tick: u64,
    window_time: u64,
}

fn nanotime() -> u64 {
    let now = Utc::now();
    // If this is negative we have other problems
    #[allow(clippy::cast_sign_loss)]
    let seconds: u64 = now.timestamp() as u64;
    let nanoseconds: u64 = u64::from(now.nanosecond());

    (seconds * 1_000_000_000) + nanoseconds
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
    /// use window::TimeWindow;
    /// TimeWindow::new(10, 100, 1000);
    /// ````

    pub fn new(size: usize, slot_time: u64, max: u64) -> Self {
        Self {
            window: SlidingWindow::new(size, max),
            window_time: slot_time * size as u64,
            slot_time,
            last_tick: 0,
        }
    }

    pub fn max(&self) -> u64 {
        self.window.max()
    }

    pub fn set_max(&mut self, max: u64) -> &Self {
        self.window.set_max(max);
        self
    }

    /// Tries to increment the counter by 1. See `SlidingWindow::inc` for details.
    pub fn inc(&mut self) -> Result<u64, u64> {
        self.add_t(nanotime(), 1)
    }

    /// Tries to increment the counter by 1. See `SlidingWindow::inc` for details.
    pub fn inc_t(&mut self, now: u64) -> Result<u64, u64> {
        self.add_t(now, 1)
    }

    /// Tries to increment the counter by n. See `SlidingWindow::add` for details.
    pub fn add(&mut self, n: u64) -> Result<u64, u64> {
        self.add_t(nanotime(), n)
    }
    pub fn add_t(&mut self, now: u64, n: u64) -> Result<u64, u64> {
        // Convert nanosecond difference to milliseconds
        let mut delta = (now - self.last_tick) / 1_000_000;

        if delta > self.window_time {
            self.window.empty();
            self.last_tick = now;
        } else if delta > self.slot_time {
            while delta > self.slot_time {
                delta -= self.slot_time;
                self.window.tick();
            }
            self.last_tick = now;
        }
        self.window.add(n)
    }

    pub fn count(&self) -> u64 {
        self.window.count()
    }
}

#[cfg(test)]
mod tests {
    use super::{SlidingWindow, TimeWindow};
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn too_much() {
        let mut sw = SlidingWindow::new(10, 10u32);
        assert!(sw.add(20).is_err())
    }

    #[test]
    fn inc() {
        let mut sw = SlidingWindow::new(10, 10u32);
        assert_eq!(sw.inc(), Ok(1));
        assert_eq!(sw.count(), 1);
    }
    #[test]
    fn all_fields() {
        let mut sw = SlidingWindow::new(10, 10u32);
        for _i in 0..10 {
            let _ = sw.tick().inc();
        }
        assert_eq!(sw.count(), 10);
    }

    #[test]
    fn rollover() {
        let mut sw = SlidingWindow::new(10, 10u32);
        for _i in 0..20 {
            let _ = sw.tick().inc();
        }
        assert_eq!(sw.count(), 10);
    }

    #[test]
    fn no_overfill() {
        let mut sw = SlidingWindow::new(10, 10u32);
        for _i in 0..10 {
            let _ = sw.tick().add(2);
        }
        assert_eq!(sw.count(), 10);
    }

    // This test is rather hideous but since we are testing
    // a time based implementaiton we got to sleep a little.
    //
    // Perhaps some time in the future there is time to
    // investigate a if there is a way to shift time forcefully.

    // We take this out since it's not reliable at the moment
    #[test]
    fn timer() {
        let mut time = super::nanotime();
        let mut tw = TimeWindow::new(20, 10, 10);
        for _i in 0..10 {
            time += 21_000_000;
            let _ = tw.inc_t(time);
        }
        assert_eq!(tw.count(), 10);
        assert!(tw.inc_t(time).is_err());
        println!("{:?}", tw);
        time += 21_000_000;
        sleep(Duration::new(0, 21_000_000));
        let r = tw.inc_t(time);
        println!("{:?}", tw);
        assert!(r.is_ok());
    }
}

#[cfg(test)]
mod properties {
    use super::SlidingWindow;
    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    enum WindowAction {
        //        Add(u8),
        Inc,
        Tick,
    }

    #[derive(Clone, Debug)]
    enum Window {
        New,
        Step {
            next: Vec<Window>,
            step: WindowAction,
        },
    }

    impl Window {
        pub fn execute(&mut self, size: usize, max: u32) -> SlidingWindow<u32> {
            match self {
                Window::New => SlidingWindow::new(size, max),
                Window::Step { next, step } => {
                    if let Some(mut next) = next.pop() {
                        let mut next = next.execute(size, max);
                        match step {
                            WindowAction::Tick => {
                                next.tick();
                                next
                            }
                            WindowAction::Inc => {
                                let _ = next.inc();
                                next
                            }
                        }
                    } else {
                        unreachable!("This should not happen!")
                    }
                }
            }
        }
    }

    fn a_window() -> BoxedStrategy<Window> {
        let leaf = prop_oneof![Just(Window::New)];
        leaf.prop_recursive(8, 256, 1, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 1).prop_map(|n| Window::Step {
                    next: n,
                    step: WindowAction::Tick
                }),
                prop::collection::vec(inner.clone(), 1).prop_map(|n| Window::Step {
                    next: n,
                    step: WindowAction::Inc
                })
            ]
        })
        .boxed()
    }
    proptest! {
        #[test]
        fn always(size in 10..200,
                  _max in 10..1000,
                  ref s in a_window()) {
            let mut s = s.clone();
            let max = 1000;
            let mut w = s.execute(size as usize, max as u32);
            if w.count() == max {
                prop_assert!(w.inc().is_err())
            } else {
                prop_assert!(w.inc().is_ok())
            }
        }
    }
}
