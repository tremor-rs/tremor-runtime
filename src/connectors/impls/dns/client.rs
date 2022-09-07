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

use crate::{connectors::prelude::*, system::KillSwitch};
use async_std::channel::{bounded, Receiver, Sender};
use async_std_resolver::{
    lookup::Lookup,
    proto::rr::{RData, RecordType},
    resolver_from_system_conf, AsyncStdResolver,
};
use std::boxed::Box;

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "dns_client".into()
    }

    async fn build(
        &self,
        _id: &Alias,
        _raw_config: &ConnectorConfig,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let (tx, rx) = bounded(128);
        Ok(Box::new(Client { tx, rx }))
    }
}

pub(crate) struct Client {
    tx: Sender<SourceReply>,
    rx: Receiver<SourceReply>,
}

#[async_trait::async_trait()]
impl Connector for Client {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        // this is a dumb source that is simply forwarding `SourceReply`s it receives from the sink
        let source = ChannelSource::from_channel(self.tx.clone(), self.rx.clone());
        builder.spawn(source, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        // issues DNS queries and forwards the responses to the source
        let s = DnsSink::new(self.tx.clone());
        builder.spawn(s, sink_context).map(Some)
    }
}

struct DnsSink {
    // for forwarding DNS responses
    tx: Sender<SourceReply>,
    resolver: Option<AsyncStdResolver>,
    origin_uri: EventOriginUri,
}

impl DnsSink {
    fn new(tx: Sender<SourceReply>) -> Self {
        let origin_uri = EventOriginUri {
            scheme: "tremor-dns".to_string(),
            host: hostname(),
            port: None,
            path: Vec::new(),
        };
        Self {
            tx,
            resolver: None,
            origin_uri,
        }
    }
    async fn query<'event>(
        &self,
        name: &str,
        record_type: Option<RecordType>,
        correlation: Option<&Value<'event>>,
    ) -> Result<EventPayload> {
        // check if we have a resolver
        let resolver = self
            .resolver
            .as_ref()
            .ok_or_else(|| Error::from("No DNS resolver available"))?;

        let data = if let Some(record_type) = record_type {
            // type lookup
            lookup_to_value(&resolver.lookup(name, record_type).await?)
        } else {
            // generic lookup
            lookup_to_value(resolver.lookup_ip(name).await?.as_lookup())
        };
        let meta = correlation.map_or_else(
            Value::object,
            |c| literal!({ "correlation": c.clone_static() }),
        );
        let e = (data, meta).into();

        Ok(e)
    }
}
#[async_trait::async_trait]
impl Sink for DnsSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        self.resolver = Some(resolver_from_system_conf().await?);
        Ok(true)
    }
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        for (_, m) in event.value_meta_iter() {
            // verify incoming request and extract DNS query params
            let dns_meta = m.get("dns");
            let lookup = dns_meta.get("lookup").ok_or("Invalid DNS request")?;
            let name = lookup
                .as_str()
                .or_else(|| lookup.get_str("name"))
                .ok_or("Invalid DNS request: `dns.lookup` missing")?;
            let record_type = lookup.get_str("type").map(str_to_record_type).transpose()?;
            // issue DNS query
            let (port, payload) = match self.query(name, record_type, m.get("correlation")).await {
                Ok(payload) => (OUT, payload),
                Err(err) => {
                    error!("{ctx} DNS Error: {err}");
                    // TODO: check for errors that require a reconnect
                    let data = literal!({
                        "request": m.get("dns").map(Value::clone_static).unwrap_or_default(),
                        "error": format!("{}", err),
                    });
                    let meta = m.get("correlation").map_or_else(
                        Value::object,
                        |c| literal!({ "correlation": c.clone_static()}),
                    );

                    let error_e = (data, meta).into();
                    (ERR, error_e)
                }
            };
            let source_reply = SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                payload,
                stream: DEFAULT_STREAM_ID,
                port: Some(port),
            };
            self.tx.send(source_reply).await?;
        }
        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

fn str_to_record_type(s: &str) -> Result<RecordType> {
    match s {
        "A" => Ok(RecordType::A),
        "AAAA" => Ok(RecordType::AAAA),
        "ANAME" => Ok(RecordType::ANAME),
        "ANY" => Ok(RecordType::ANY),
        "AXFR" => Ok(RecordType::AXFR),
        "CAA" => Ok(RecordType::CAA),
        "CNAME" => Ok(RecordType::CNAME),
        "HINFO" => Ok(RecordType::HINFO),
        "HTTPS" => Ok(RecordType::HTTPS),
        "IXFR" => Ok(RecordType::IXFR),
        "MX" => Ok(RecordType::MX),
        "NAPTR" => Ok(RecordType::NAPTR),
        "NS" => Ok(RecordType::NS),
        "NULL" => Ok(RecordType::NULL),
        "OPENPGPKEY" => Ok(RecordType::OPENPGPKEY),
        "OPT" => Ok(RecordType::OPT),
        "PTR" => Ok(RecordType::PTR),
        "SOA" => Ok(RecordType::SOA),
        "SRV" => Ok(RecordType::SRV),
        "SSHFP" => Ok(RecordType::SSHFP),
        "SVCB" => Ok(RecordType::SVCB),
        "TLSA" => Ok(RecordType::TLSA),
        "TXT" => Ok(RecordType::TXT),
        "ZERO" => Ok(RecordType::ZERO),
        other => Err(format!("Invalid or unsupported record type: {}", other).into()),
    }
}

fn rdata_to_value(r: &RData) -> Option<Value<'static>> {
    Some(match r {
        RData::A(v) => literal!({ "A": v.to_string() }),
        RData::AAAA(v) => literal!({ "AAAA": v.to_string() }),
        RData::ANAME(v) => literal!({ "ANAME": v.to_string() }),
        RData::CNAME(v) => literal!({ "CNAME": v.to_string() }),
        RData::TXT(v) => literal!({ "TXT": v.to_string() }),
        RData::PTR(v) => literal!({ "PTR": v.to_string() }),
        RData::CAA(v) => literal!({ "CAA": v.to_string() }),
        RData::HINFO(v) => literal!({ "HINFO": v.to_string() }),
        RData::HTTPS(v) => literal!({ "HTTPS": v.to_string() }),
        RData::MX(v) => literal!({ "MX": v.to_string() }),
        RData::NAPTR(v) => literal!({ "NAPTR": v.to_string() }),
        RData::NULL(v) => literal!({ "NULL": v.to_string() }),
        RData::NS(v) => literal!({ "NS": v.to_string() }),
        RData::OPENPGPKEY(v) => literal!({ "OPENPGPKEY": v.to_string() }),
        RData::SOA(v) => literal!({ "SOA": v.to_string() }),
        RData::SRV(v) => literal!({ "SRV": v.to_string() }),
        RData::SSHFP(v) => literal!({ "SSHFP": v.to_string() }),
        RData::SVCB(v) => literal!({ "SVCB": v.to_string() }),
        RData::TLSA(v) => literal!({ "TLSA": v.to_string() }),
        RData::CSYNC(v) => literal!({"CSYNC": v.to_string() }),
        // RData marked as non-exhaustive
        _ => return None,
    })
}

fn lookup_to_value(l: &Lookup) -> Value<'static> {
    l.record_iter()
        .filter_map(|r| {
            let ttl = r.ttl();
            r.data()
                .and_then(rdata_to_value)
                .and_then(|mut v| v.try_insert("ttl", ttl))
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_str_to_record_type() {
        assert_eq!(str_to_record_type("A"), Ok(RecordType::A));
        assert_eq!(str_to_record_type("AAAA"), Ok(RecordType::AAAA));
        assert_eq!(str_to_record_type("ANAME"), Ok(RecordType::ANAME));
        assert_eq!(str_to_record_type("ANY"), Ok(RecordType::ANY));
        assert_eq!(str_to_record_type("AXFR"), Ok(RecordType::AXFR));
        assert_eq!(str_to_record_type("CAA"), Ok(RecordType::CAA));
        assert_eq!(str_to_record_type("CNAME"), Ok(RecordType::CNAME));
        assert_eq!(str_to_record_type("HINFO"), Ok(RecordType::HINFO));
        assert_eq!(str_to_record_type("HTTPS"), Ok(RecordType::HTTPS));
        assert_eq!(str_to_record_type("IXFR"), Ok(RecordType::IXFR));
        assert_eq!(str_to_record_type("MX"), Ok(RecordType::MX));
        assert_eq!(str_to_record_type("NAPTR"), Ok(RecordType::NAPTR));
        assert_eq!(str_to_record_type("NS"), Ok(RecordType::NS));
        assert_eq!(str_to_record_type("NULL"), Ok(RecordType::NULL));
        assert_eq!(str_to_record_type("OPENPGPKEY"), Ok(RecordType::OPENPGPKEY));
        assert_eq!(str_to_record_type("OPT"), Ok(RecordType::OPT));
        assert_eq!(str_to_record_type("PTR"), Ok(RecordType::PTR));
        assert_eq!(str_to_record_type("SOA"), Ok(RecordType::SOA));
        assert_eq!(str_to_record_type("SRV"), Ok(RecordType::SRV));
        assert_eq!(str_to_record_type("SSHFP"), Ok(RecordType::SSHFP));
        assert_eq!(str_to_record_type("SVCB"), Ok(RecordType::SVCB));
        assert_eq!(str_to_record_type("TLSA"), Ok(RecordType::TLSA));
        assert_eq!(str_to_record_type("TXT"), Ok(RecordType::TXT));
        assert_eq!(str_to_record_type("ZERO"), Ok(RecordType::ZERO));
        assert!(str_to_record_type("NOT A DNS ENTRIE").is_err());
    }
}
