use std::time::SystemTime;

/// Get a nanosecond timestamp
#[must_use]
pub fn nanotime() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        // ALLOW: If this happens, now() is BEFORE the unix epoch, this is so bad panicing is the least of our problems
        .expect("Our time was before the unix epoc, this is really bad!")
        .as_nanos() as u64
}
