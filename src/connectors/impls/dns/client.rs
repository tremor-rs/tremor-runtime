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

use crate::connectors::prelude::*;
use async_std::channel::{bounded, Receiver, Sender, TryRecvError};
use async_std_resolver::{
    lookup::Lookup,
    proto::{
        rr::{RData, RecordType},
        xfer::DnsRequestOptions,
    },
    resolver_from_system_conf, AsyncStdResolver,
};
use beef::Cow;
use std::boxed::Box;

type DnsMesssage = (Cow<'static, str>, EventPayload);

pub struct DnsClient {
    tx: Sender<DnsMesssage>,
    rx: Receiver<DnsMesssage>,
    origin_uri: EventOriginUri,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "dns_client".into()
    }

    async fn from_config(
        &self,
        _id: &TremorUrl,
        _raw_config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        let (tx, rx) = bounded(128);
        let origin_uri = EventOriginUri {
            scheme: "tremor-dns".to_string(),
            host: "localhost".to_string(),
            port: None,
            path: Vec::new(),
        };

        Ok(Box::new(DnsClient { tx, rx, origin_uri }))
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
        RData::OPT(_) | RData::Unknown { .. } | RData::ZERO => return None,
    })
}

fn lookup_to_value(l: &Lookup) -> Value<'static> {
    l.record_iter()
        .filter_map(|r| {
            let mut v = rdata_to_value(r.rdata())?;
            v.try_insert("ttl", r.ttl());
            Some(v)
        })
        .collect()
}

struct DnsSource {
    rx: Receiver<DnsMesssage>,
    origin_uri: EventOriginUri,
}

#[async_trait::async_trait()]
impl Source for DnsSource {
    fn is_transactional(&self) -> bool {
        false
    }
    async fn pull_data(&mut self, _pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        match self.rx.try_recv() {
            Ok((port, payload)) => Ok(SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                stream: 0,
                payload,
                port: Some(port),
            }),
            Err(TryRecvError::Empty) => {
                // TODO: configure pull interval in connector config?
                Ok(SourceReply::Empty(DEFAULT_POLL_INTERVAL))
            }
            Err(e) => Err(e.into()),
        }
    }
}

struct DnsSink {
    tx: Sender<DnsMesssage>,
    resolver: AsyncStdResolver,
}

impl DnsSink {
    async fn query<'event>(
        &self,
        request: Option<&Value<'event>>,
        correlation: Option<&Value<'event>>,
    ) -> Result<EventPayload> {
        let lookup = request.ok_or("Invalid DNS request")?;
        let name = lookup
            .as_str()
            .or_else(|| lookup.get_str("name"))
            .ok_or("Invaliud DNS request: `connector.dns.lookup` missing")?;

        let data = if let Some(record_type) =
            lookup.get_str("type").map(str_to_record_type).transpose()?
        {
            // type lookup
            lookup_to_value(
                &self
                    .resolver
                    .lookup(name, record_type, DnsRequestOptions::default())
                    .await?,
            )
        } else {
            // generic lookup
            lookup_to_value(self.resolver.lookup_ip(name).await?.as_lookup())
        };
        let meta = correlation
            .map(|c| literal!({ "correlation": c.clone_static() }))
            .unwrap_or_else(|| Value::object());
        let e = (data, meta).into();

        Ok(e)
    }
}
#[async_trait::async_trait]
impl Sink for DnsSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        for (_, m) in event.value_meta_iter() {
            match self
                .query(
                    m.get("connector").get("dns").get("lookup"),
                    m.get("correlation"),
                )
                .await
            {
                Ok(e) => self.tx.send((OUT, e)).await?,
                Err(err) => {
                    error!("DNS Error: {}", err);
                    let data = literal!({
                        "request": m.get("connector").get("dns").map(Value::clone_static).unwrap_or_default(),
                        "error": format!("{}", err),
                    });
                    let meta = m
                        .get("correlation")
                        .map(|c| literal!({ "correlation": c.clone_static() }))
                        .unwrap_or_else(|| Value::object());

                    let error_e = (data, meta).into();

                    self.tx.send((ERR, error_e)).await?;
                }
            }
        }
        Ok(SinkReply::default())
    }

    fn auto_ack(&self) -> bool {
        true
    }
}
#[async_trait::async_trait()]
impl Connector for DnsClient {
    fn is_structured(&self) -> bool {
        true
    }
    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let s = DnsSource {
            rx: self.rx.clone(),
            origin_uri: self.origin_uri.clone(),
        };
        builder.spawn(s, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let resolver = resolver_from_system_conf().await?;
        let s = DnsSink {
            resolver,
            tx: self.tx.clone(),
        };
        builder.spawn(s, sink_context).map(Some)
    }
}
