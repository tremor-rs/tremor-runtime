use chrono::Timelike;
use chrono::Utc;
use hdrhistogram::serialization::{DeserializeError, Deserializer};
use hdrhistogram::Histogram;
use libc;
use std::fmt::Display;
use std::io;
use std::io::Read;
use std::io::Write;
use std::ptr;
use std::time::Duration;

pub fn duration_to_millis(at: Duration) -> u64 {
    (at.as_secs() as u64 * 1_000) + (u64::from(at.subsec_nanos()) / 1_000_000)
}

pub fn nanos_to_millis(nanos: u64) -> u64 {
    nanos / 1_000_000
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

    (seconds * 1_000_000_000) + nanoseconds
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
        (remain.tv_sec * 1_000_000_000 + remain.tv_nsec) as u64,
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

#[derive(Debug)]
pub enum HistogramError {
    Io(io::Error),
    // HistogramSerialize(V2SerializeError),
    HistogramDeserialize(DeserializeError),
}

pub fn quantiles<R: Read, W: Write>(
    mut reader: R,
    mut writer: W,
    quantile_precision: usize,
    ticks_per_half: u32,
) -> Result<(), HistogramError> {
    let hist: Histogram<u64> = Deserializer::new().deserialize(&mut reader)?;

    writer.write_all(
        format!(
            "{:>10} {:>quantile_precision$} {:>10} {:>14}\n\n",
            "Value",
            "Percentile",
            // "QuantileIteration",
            "TotalCount",
            "1/(1-Percentile)",
            quantile_precision = quantile_precision + 2 // + 2 from leading "0." for numbers
        ).as_ref(),
    )?;
    let mut sum = 0;
    for v in hist.iter_quantiles(ticks_per_half) {
        sum += v.count_since_last_iteration();
        if v.quantile_iterated_to() < 1.0 {
            writer.write_all(
                format!(
                    "{:12} {:1.*} {:10} {:14.2}\n",
                    v.value_iterated_to(),
                    quantile_precision,
                    //                        v.quantile(),
                    //                        quantile_precision,
                    v.quantile_iterated_to(),
                    sum,
                    1_f64 / (1_f64 - v.quantile_iterated_to()),
                ).as_ref(),
            )?;
        } else {
            writer.write_all(
                format!(
                    "{:12} {:1.*} {:10} {:>14}\n",
                    v.value_iterated_to(),
                    quantile_precision,
                    //                        v.quantile(),
                    //                        quantile_precision,
                    v.quantile_iterated_to(),
                    sum,
                    "inf"
                ).as_ref(),
            )?;
        }
    }

    fn write_extra_data<T1: Display, T2: Display, W: Write>(
        writer: &mut W,
        label1: &str,
        data1: T1,
        label2: &str,
        data2: T2,
    ) -> Result<(), io::Error> {
        writer.write_all(
            format!(
                "#[{:10} = {:12.2}, {:14} = {:12.2}]\n",
                label1, data1, label2, data2
            ).as_ref(),
        )
    }

    write_extra_data(
        &mut writer,
        "Mean",
        hist.mean(),
        "StdDeviation",
        hist.stdev(),
    )?;
    write_extra_data(&mut writer, "Max", hist.max(), "Total count", hist.len())?;
    write_extra_data(
        &mut writer,
        "Buckets",
        hist.buckets(),
        "SubBuckets",
        hist.distinct_values(),
    )?;

    Ok(())
}

impl From<io::Error> for HistogramError {
    fn from(e: io::Error) -> Self {
        HistogramError::Io(e)
    }
}

impl From<DeserializeError> for HistogramError {
    fn from(e: DeserializeError) -> Self {
        HistogramError::HistogramDeserialize(e)
    }
}
