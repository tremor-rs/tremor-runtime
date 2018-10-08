extern crate mimir;
extern crate rdkafka;

use rdkafka::util::get_rdkafka_version;
use std::io::Write;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

/// println_stderr and run_command_or_fail are copied from rdkafka-sys
macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

pub fn print() {
    println_stderr!("tremor version: {}", VERSION);
    println_stderr!("mimir version: {}", mimir::version());
    let (version_n, version_s) = get_rdkafka_version();
    println_stderr!("rd_kafka version: 0x{:08x}, {}", version_n, version_s);
}
