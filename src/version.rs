extern crate mimir;
extern crate rdkafka;

use rdkafka::util::get_rdkafka_version;

pub fn print() {
    println!("mimir version: {}", mimir::version());
    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);
}
