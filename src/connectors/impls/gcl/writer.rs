// Copyright 2022, The Tremor Team
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

pub(crate) mod meta;
mod sink;

use crate::connectors::google::{GouthTokenProvider, TokenSrc};
use crate::connectors::impls::gcl::writer::sink::{GclSink, TonicChannelFactory};
use crate::connectors::prelude::*;
use crate::connectors::{Alias, Connector, ConnectorBuilder, ConnectorConfig, ConnectorType};
use crate::errors::Error;
use googapis::google::api::MonitoredResource;
use googapis::google::logging::r#type::LogSeverity;
use serde::Deserialize;
use simd_json::OwnedValue;
use std::collections::HashMap;
use tonic::transport::Channel;
use tremor_pipeline::ConfigImpl;

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// The default `log_name` for this configuration or `default` if not provided.
    /// The `log_name` field can be overridden in metadata.
    ///
    /// From the protocol buffers specification:
    ///
    /// Optional. A default log resource name that is assigned to all log entries in entries that do not specify a value for log_name:
    ///
    /// "projects/\[PROJECT_ID]/logs/[LOG_ID\]"
    /// "organizations/\[ORGANIZATION_ID]/logs/[LOG_ID\]"
    /// "billingAccounts/\[BILLING_ACCOUNT_ID]/logs/[LOG_ID\]"
    /// "folders/\[FOLDER_ID]/logs/[LOG_ID\]"
    /// \[LOG_ID\] must be URL-encoded. For example:
    ///
    /// "projects/my-project-id/logs/syslog"
    /// "organizations/1234567890/logs/cloudresourcemanager.googleapis.com%2Factivity"
    /// The permission logging.logEntries.create is needed on each project, organization, billing account, or folder that is receiving new log entries, whether the resource is specified in logName or in an individual log entry.
    pub log_name: Option<String>,

    /// The default monitored resource for the log entries being written.
    ///
    /// From the protocol buffers specification:
    ///
    /// Optional. A default monitored resource object that is assigned to all log entries in entries that do not specify a value for resource. Example:
    ///
    /// ```json
    /// { "type": "gce_instance",
    /// "labels": {
    ///   "zone": "us-central1-a", "instance_id": "00000000000000000000" }}
    /// ```    
    #[serde(default = "Default::default")]
    pub resource: Option<simd_json::OwnedValue>,

    /// This setting sets the behaviour of the connector with respect to whether valid entries should
    /// be written even if some entries in a batch set to Google Cloud Logging are invalid. Defaults to
    /// false
    ///
    /// From the protocol buffers specification:
    ///
    /// Optional. Whether valid entries should be written even if some other entries fail due to INVALID_ARGUMENT or PERMISSION_DENIED errors. If any entry is not written, then the response status is the error associated with one of the failed entries and the response includes error details keyed by the entries' zero-based index in the entries.write method.
    #[serde(default = "default_partial_success")]
    pub partial_success: bool,

    /// This setting enables a sanity check for validating that log entries are well formed and
    /// and valid by exercising the connector to write entries where resulting entries are not
    /// persisted. Useful primarily during initial exploration and configuration or after
    /// large configuration changes as a sanity check.
    ///
    /// From the protocol buffers specification:
    ///
    /// Optional. If true, the request should expect normal response, but the entries won't be persisted nor exported. Useful for checking whether the logging API endpoints are working properly before sending valuable data.
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,

    /// Sets a connection timeout for connections to the GCP logging endpoint
    /// that applies during connection
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64,

    /// Sets a request timeout for requests to the GCP logging endpoint
    /// that applies for each request sent to the endpoint
    #[serde(default = "default_request_timeout")]
    pub request_timeout: u64,

    /// This setting sets a default log severity that can be overriden on a per event
    /// basis through metadata
    #[serde(default = "default_log_severity")]
    pub default_severity: i32,

    /// This setting sets a default set of labels that can be overriden on a per event
    /// basis through metadata
    #[serde(default = "Default::default")]
    pub labels: HashMap<String, String>,

    /// This settings sets an upper limit on the number of concurrent in flight requests
    /// that can be in progress simultaneously
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// Token to use for authentication
    pub token: TokenSrc,
}

fn default_partial_success() -> bool {
    false
}

fn default_dry_run() -> bool {
    false
}

fn default_connect_timeout() -> u64 {
    1_000_000_000 // 1 second
}

fn default_request_timeout() -> u64 {
    10_000_000_000 // 10 seconds
}

fn default_log_severity() -> i32 {
    LogSeverity::Default as i32
}

fn default_concurrency() -> usize {
    4
}

impl Config {
    fn default_log_name(&self) -> String {
        // Use user supplied default, if provided
        if let Some(has_usersupplied_default) = &self.log_name {
            return has_usersupplied_default.clone();
        }

        // Use hardwired `default`, if no user supplied default provided
        "default".to_string()
    }

    pub(crate) fn log_name(&self, meta: Option<&Value>) -> String {
        // Override for a specific per event log_name
        if let Some(has_meta) = meta {
            if let Some(log_name) = has_meta.get("log_name") {
                return log_name.to_string();
            }
        }

        self.default_log_name()
    }

    pub(crate) fn log_severity(&self, meta: Option<&Value>) -> Result<i32> {
        // Override for a specific per event severity
        if let Some(has_meta) = meta {
            if let Some(log_severity) = has_meta.get("log_severity") {
                return log_severity
                    .as_i32()
                    .ok_or_else(|| "log_severity is not an integer".into());
            };
        }

        Ok(self.default_severity)
    }

    pub(crate) fn labels(meta: Option<&Value>) -> HashMap<String, String> {
        let mut labels = HashMap::new();

        if let Some(has_meta) = meta.get_object("labels") {
            for (k, v) in has_meta {
                labels.insert(k.to_string(), v.to_string());
            }
        }

        labels
    }
}

impl ConfigImpl for Config {}

fn value_to_monitored_resource(
    from: Option<&simd_json::OwnedValue>,
) -> Result<Option<MonitoredResource>> {
    match from {
        None => Ok(None),
        Some(from) => {
            let vt = from.value_type();
            match from {
                // Consider refactoring for unwrap_or_default when #1819 is resolved
                OwnedValue::Object(from) => {
                    let kind = from.get("type");
                    let kind = kind.as_str();
                    let maybe_labels = from.get("labels");
                    let labels: HashMap<String, String> = match maybe_labels {
                        None => HashMap::new(),
                        Some(labels) => labels
                            .as_object()
                            .ok_or_else(|| {
                                Error::from(ErrorKind::GclTypeMismatch("Value::Object", vt))
                            })?
                            .iter()
                            .map(|(key, value)| {
                                let key = key.to_string();
                                let value = value.to_string();
                                (key, value)
                            })
                            .collect(),
                    };
                    Ok(Some(MonitoredResource {
                        r#type: match kind {
                            None => String::new(),
                            Some(kind) => kind.to_string(),
                        },
                        labels,
                    }))
                }
                _otherwise => Err(Error::from(ErrorKind::GclTypeMismatch(
                    "Value::Object",
                    from.value_type(),
                ))),
            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

struct Gcl {
    config: Config,
}

#[async_trait::async_trait]
impl Connector for Gcl {
    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = GclSink::<GouthTokenProvider, Channel>::new(
            self.config.clone(),
            builder.reply_tx(),
            TonicChannelFactory,
        );

        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "gcl_writer".into()
    }

    async fn build_cfg(
        &self,
        _id: &Alias,
        _: &ConnectorConfig,
        raw: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        Ok(Box::new(Gcl {
            config: Config::new(raw)?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_to_monitored_resource_conversion() -> Result<()> {
        let mut ok_count = 0;
        let from: OwnedValue = literal!({
            "type": "gce_instance".to_string(),
            "labels": {
              "zone": "us-central1-a",
              "instance_id": "00000000000000000000",
            }
        })
        .into();
        let value = value_to_monitored_resource(Some(&from))?;
        if let Some(value) = value {
            assert_eq!("gce_instance", &value.r#type);
            assert_eq!("us-central1-a".to_string(), value.labels["zone"]);
            assert_eq!(
                "00000000000000000000".to_string(),
                value.labels["instance_id"]
            );
            ok_count += 1;
        } else {
            return Err("Skipped test asserts due to serialization error".into());
        }

        let from: OwnedValue = literal!({
            "type": "gce_instance".to_string(),
        })
        .into();
        let value = value_to_monitored_resource(Some(&from))?;
        if let Some(value) = value {
            assert_eq!(0, value.labels.len());
            ok_count += 1;
        } else {
            return Err("Skipped test asserts due to serialization error".into());
        }

        let from: OwnedValue = literal!({
            "type": "gce_instance".to_string(),
            "labels": [ "snot" ]
        })
        .into();
        let bad_labels = value_to_monitored_resource(Some(&from));
        assert!(bad_labels.is_err());

        let from = literal!(["snot"]);
        let from: OwnedValue = from.into();
        let bad_value = value_to_monitored_resource(Some(&from));
        assert!(bad_value.is_err());

        assert_eq!(2, ok_count);
        Ok(())
    }
}
