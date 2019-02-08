// Copyright 2018, Wayfair GmbH
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

use chrono::Timelike;
use chrono::Utc;
use libc;
#[cfg(test)]
use serde_yaml::Value;
use std::ptr;
use std::time::Duration;

pub fn nanos_to_millis(nanos: u64) -> u64 {
    nanos / ms!(1)
}

pub fn duration_to_millis(at: Duration) -> u64 {
    (at.as_secs() as u64 * 1_000) + (u64::from(at.subsec_nanos()) / ms!(1))
}

pub fn duration_to_libc_timespec(at: Duration) -> libc::timespec {
    libc::timespec {
        tv_sec: at.as_secs() as libc::time_t,
        tv_nsec: i64::from(at.subsec_nanos()) as libc::c_long,
    }
}

pub fn nanotime() -> u64 {
    let now = Utc::now();
    let seconds: u64 = now.timestamp() as u64;
    let nanoseconds: u64 = u64::from(now.nanosecond());

    (seconds * s!(1)) + nanoseconds
}

/// Park current thread for a precise ( nanosecond granular, more precise than sleep ) amount of time
/// Resolution varies by operating system / hardware architecture
///
pub fn park(at: Duration) -> Option<Duration> {
    let ts = duration_to_libc_timespec(at);
    let mut remain = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };

    native_park(ts, &mut remain);

    if remain.tv_nsec == 0 && remain.tv_sec == 0 {
        return None;
    }

    Some(Duration::from_nanos(
        (remain.tv_sec * s!(1) + remain.tv_nsec) as u64,
    ))
}

fn native_park(ts: libc::timespec, remain: &mut libc::timespec) {
    #[cfg(target_os = "linux")]
    linux_park(libc::CLOCK_MONOTONIC, 0, &ts, Some(remain));

    #[cfg(target_os = "macos")]
    macos_park(&ts, Some(remain));
}

///
///        Park a thread for a precise aount of time on Linux
///
///        int clock_nanosleep(clockid_t clock_id, int flags,
///                           const struct timespec *request,
///                           struct timespec *remain);
///
#[cfg(target_os = "linux")]
fn linux_park(
    clock_id: libc::clockid_t,
    flags: libc::c_int,
    request: &libc::timespec,
    remain: Option<&mut libc::timespec>,
) -> i32 {
    match remain {
        Some(p) => unsafe {
            libc::clock_nanosleep(clock_id, flags, request as *const _, p as *mut _)
        },
        _ => unsafe {
            libc::clock_nanosleep(clock_id, flags, request as *const _, ptr::null_mut())
        },
    }
}

///
///         Park a thread for a precise amount of time on Mac OS X
///
///         int
///             nanosleep(const struct timespec *rqtp, struct timespec *rmtp);
///
#[cfg(target_os = "macos")]
fn macos_park(rqtp: &libc::timespec, remain: Option<&mut libc::timespec>) -> i32 {
    match remain {
        Some(rmtp) => unsafe { libc::nanosleep(rqtp as *const _, rmtp as *mut _) },
        _ => unsafe { libc::nanosleep(rqtp as *const _, ptr::null_mut()) },
    }
}

#[cfg(test)]
pub fn vs(s: &str) -> Value {
    Value::String(s.to_string())
}

#[cfg(test)]
pub fn vi(i: u64) -> Value {
    Value::Number(i.into())
}

#[cfg(test)]
pub fn vf(f: f64) -> Value {
    Value::Number(f.into())
}
