use log::LevelFilter;
use log4rs::{
    config::{Appender, Root},
    Config,
};
use tremor_pipeline::{PluggableLoggingAppender, LOGGING_CHANNEL};

pub fn run() {
    let tx = LOGGING_CHANNEL.tx();

    let stdout = PluggableLoggingAppender { tx };

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Info))
        .unwrap();

    log4rs::init_config(config).unwrap();
}
