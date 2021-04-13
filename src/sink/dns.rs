// Copyright 2020-2021, The Tremor Team
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

// TODO: Add correlation of reads and replies.

#![cfg(not(tarpaulin_include))]
use crate::sink::{prelude::*, Reply};
use crate::source::prelude::*;
use async_channel::Sender;
use async_std_resolver::{
    lookup::Lookup,
    proto::{
        rr::{RData, RecordType},
        xfer::DnsRequestOptions,
    },
    resolver_from_system_conf, AsyncStdResolver,
};
use halfbrown::HashMap;
use std::boxed::Box;

pub struct Dns {
    // sink_url: TremorUrl,
    event_origin_uri: EventOriginUri,
    resolver: Option<AsyncStdResolver>,
    // reply: Option<Sender<Reply>>,
}

impl offramp::Impl for Dns {
    fn from_config(_config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        let event_origin_uri = EventOriginUri {
            uid: 0,
            scheme: "tremor-dns".to_string(),
            host: "localhost".to_string(),
            port: None,
            path: Vec::new(),
        };

        Ok(SinkManager::new_box(Dns {
            event_origin_uri,
            resolver: None,
        }))
    }
}

fn str_to_record_type(s: &str) -> Option<RecordType> {
    match s {
        "A" => Some(RecordType::A),
        "AAAA" => Some(RecordType::AAAA),
        "ANAME" => Some(RecordType::ANAME),
        "ANY" => Some(RecordType::ANY),
        "AXFR" => Some(RecordType::AXFR),
        "CAA" => Some(RecordType::CAA),
        "CNAME" => Some(RecordType::CNAME),
        "HINFO" => Some(RecordType::HINFO),
        "HTTPS" => Some(RecordType::HTTPS),
        "IXFR" => Some(RecordType::IXFR),
        "MX" => Some(RecordType::MX),
        "NAPTR" => Some(RecordType::NAPTR),
        "NS" => Some(RecordType::NS),
        "NULL" => Some(RecordType::NULL),
        "OPENPGPKEY" => Some(RecordType::OPENPGPKEY),
        "OPT" => Some(RecordType::OPT),
        "PTR" => Some(RecordType::PTR),
        "SOA" => Some(RecordType::SOA),
        "SRV" => Some(RecordType::SRV),
        "SSHFP" => Some(RecordType::SSHFP),
        "SVCB" => Some(RecordType::SVCB),
        "TLSA" => Some(RecordType::TLSA),
        "TXT" => Some(RecordType::TXT),
        "ZERO" => Some(RecordType::ZERO),
        _ => None,
    }
}

fn rdata_to_value(r: &RData) -> Option<Value<'static>> {
    use simd_json::json_typed;
    Some(match r {
        RData::A(v) => json_typed!(borrowed, { "A": v.to_string() }).into(),
        RData::AAAA(v) => json_typed!(borrowed, { "AAAA": v.to_string() }).into(),
        RData::ANAME(v) => json_typed!(borrowed, { "ANAME": v.to_string() }).into(),
        RData::CNAME(v) => json_typed!(borrowed, { "CNAME": v.to_string() }).into(),
        RData::TXT(v) => json_typed!(borrowed, { "TXT": v.to_string() }).into(),
        RData::PTR(v) => json_typed!(borrowed, { "PTR": v.to_string() }).into(),
        RData::CAA(v) => json_typed!(borrowed, { "CAA": v.to_string() }).into(),
        RData::HINFO(v) => json_typed!(borrowed, { "HINFO": v.to_string() }).into(),
        RData::HTTPS(v) => json_typed!(borrowed, { "HTTPS": v.to_string() }).into(),
        RData::MX(v) => json_typed!(borrowed, { "MX": v.to_string() }).into(),
        RData::NAPTR(v) => json_typed!(borrowed, { "NAPTR": v.to_string() }).into(),
        RData::NULL(v) => json_typed!(borrowed, { "NULL": v.to_string() }).into(),
        RData::NS(v) => json_typed!(borrowed, { "NS": v.to_string() }).into(),
        RData::OPENPGPKEY(v) => json_typed!(borrowed, { "OPENPGPKEY": v.to_string() }).into(),
        RData::SOA(v) => json_typed!(borrowed, { "SOA": v.to_string() }).into(),
        RData::SRV(v) => json_typed!(borrowed, { "SRV": v.to_string() }).into(),
        RData::SSHFP(v) => json_typed!(borrowed, { "SSHFP": v.to_string() }).into(),
        RData::SVCB(v) => json_typed!(borrowed, { "SVCB": v.to_string() }).into(),
        RData::TLSA(v) => json_typed!(borrowed, { "TLSA": v.to_string() }).into(),
        RData::OPT(_) | RData::Unknown { .. } | RData::ZERO => return None,
    })
}
fn lookup_to_value(l: &Lookup) -> Value<'static> {
    l.iter().filter_map(rdata_to_value).collect()
}

#[async_trait::async_trait]
impl Sink for Dns {
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        let resolver = self
            .resolver
            .as_ref()
            .ok_or_else(|| Error::from("No resolver set"))?;
        let mut res = Vec::with_capacity(event.len());
        let correlation = event.correlation_meta();

        for e in event.value_iter() {
            if let Some(lookup) = e.get("lookup") {
                if let Some(name) = lookup.as_str().or_else(|| lookup.get_str("name")) {
                    let data = if let Some(record_type) =
                        lookup.get_str("type").and_then(str_to_record_type)
                    {
                        // type lookup
                        lookup_to_value(
                            &resolver
                                .lookup(name, record_type, DnsRequestOptions::default())
                                .await?,
                        )
                    } else {
                        // generic lookup
                        lookup_to_value(resolver.lookup_ip(name).await?.as_lookup())
                    };
                    let e = Event {
                        data: (data, correlation.clone().unwrap_or_default()).into(),
                        origin_uri: Some(self.event_origin_uri.clone()),
                        ..Event::default()
                    };

                    res.push(Reply::Response("out".into(), e))
                } else {
                    error!("Invalid DNS request: {}", e)
                }
            }
        }
        Ok(Some(res))
    }

    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None)
    }

    async fn init(
        &mut self,
        _sink_uid: u64,
        _sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        _processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<Reply>,
    ) -> Result<()> {
        // self.reply = Some(reply_channel);
        self.resolver = Some(resolver_from_system_conf().await?);
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.resolver.is_some()
    }

    fn auto_ack(&self) -> bool {
        true
    }

    fn default_codec(&self) -> &str {
        "null"
    }
}
