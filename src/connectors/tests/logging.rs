// Copyright 2021, The Tremor Team
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

use async_broadcast::{broadcast, Receiver};
use log::{LevelFilter, Log};
use log4rs::{
    config::{Appender, Config, Root},
    Logger,
};
use serial_test::serial;
use tremor_pipeline::{PluggableLoggingAppender, LoggingChannel, LoggingMsg};
use tremor_script::literal;

fn append_channel() -> (Logger,  Receiver<LoggingMsg>) {

	let ( tx, rx) = broadcast(128);

	let logging_channel =LoggingChannel {
		tx: tx,
		rx: rx,
	};

    let stdout = PluggableLoggingAppender { tx:logging_channel.tx() };

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Trace))
        .unwrap();

    (Logger::new(config), logging_channel.rx())
}

#[async_std::test]
#[serial(logging)]
async fn log_info() {
    let (logger, mut receiver) = append_channel();
	let r = &Record::builder()
	.args(format_args!("Info!"))
	.level(Level::Info)
	.file(Some("src/connectors/tests/logging.rs"))
	.line(Some(1000))
	.module_path(Some("tremor_runtime::connectors::tests::logging"))
	.build();
	logger.log(r);
	let v = receiver.recv().await.unwrap();
    let value = v.payload.suffix().value();
    let t = &literal!({"level": "INFO", "args": "Info!","path": "tremor_runtime::connectors::tests::logging", "file":"src/connectors/tests/logging.rs","line": "1000"});
	assert_eq!(value, t);

}

#[async_std::test]
#[serial(logging)]
async fn log_warn() {
    let (logger, mut receiver) = append_channel();
	let r = &Record::builder()
	.args(format_args!("Warn!"))
	.level(Level::Warn)
	.file(Some("src/connectors/tests/logging.rs"))
	.line(Some(1000))
	.module_path(Some("tremor_runtime::connectors::tests::logging"))
	.build();
	logger.log(r);
	let v = receiver.recv().await.unwrap();
    let value = v.payload.suffix().value();
    let t = &literal!({"level": "WARN", "args": "Warn!","path": "tremor_runtime::connectors::tests::logging", "file":"src/connectors/tests/logging.rs","line": "1000"});
    assert_eq!(value, t);
}

#[async_std::test]
#[serial(logging)]
async fn log_error() {
    let (logger, mut receiver) = append_channel();
	let r = &Record::builder()
	.args(format_args!("Error!"))
	.level(Level::Error)
	.file(Some("src/connectors/tests/logging.rs"))
	.line(Some(1000))
	.module_path(Some("tremor_runtime::connectors::tests::logging"))
	.build();
	logger.log(r);
	let v = receiver.recv().await.unwrap();
    let value = v.payload.suffix().value();
    let t = &literal!({"level": "ERROR", "args": "Error!","path": "tremor_runtime::connectors::tests::logging", "file":"src/connectors/tests/logging.rs","line": "1000"});
    assert_eq!(value, t);
}

#[async_std::test]
#[serial(logging)]
async fn log_debug() {
    let (logger, mut receiver )= append_channel();
	let r = &Record::builder()
	.args(format_args!("Debug!"))
	.level(Level::Debug)
	.file(Some("src/connectors/tests/logging.rs"))
	.line(Some(1000))
	.module_path(Some("tremor_runtime::connectors::tests::logging"))
	.build();
 	logger.log(&r);
    let v = receiver.recv().await.unwrap();
    let value = v.payload.suffix().value();
    let t = &literal!({"level": "DEBUG", "args": "Debug!","path": "tremor_runtime::connectors::tests::logging", "file":"src/connectors/tests/logging.rs","line": "1000"});
    assert_eq!(value, t);
}

#[async_std::test]
#[serial(logging)]
async fn log_trace() {
    let (logger, mut receiver )= append_channel();
	let r = &Record::builder()
	.args(format_args!("Trace!"))
	.level(Level::Trace)
	.file(Some("src/connectors/tests/logging.rs"))
	.line(Some(1000))
	.module_path(Some("tremor_runtime::connectors::tests::logging"))
	.build();
 	logger.log(&r);
    let v = receiver.recv().await.unwrap();
    let value = v.payload.suffix().value();
    let t = &literal!({"level": "TRACE", "args": "Trace!","path": "tremor_runtime::connectors::tests::logging", "file":"src/connectors/tests/logging.rs","line": "1000"});
    assert_eq!(value, t);
}

