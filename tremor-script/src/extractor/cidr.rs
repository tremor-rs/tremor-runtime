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
// See the License for the specific language

//! Classless Inter-Domain Routing ( CIDR ) is a method of allocating IP addresses and IP routing paths. CIDR notation is a compact representation of an IP address and its associated routing prefix.
//!
//! The notation is constructed from a possibly incomplete IP address, followed by a slash (`/`) character, and then a decimal number.
//!
//! The number is the count of leading *1* bits in the subnet mask demarcating routing boundaries for the range of addresses being routed over.
//!
//! Larger values here indicate smaller networks. The maximum size of the network is given by the number of addresses that are possible with the remaining, least-significant bits below the prefix.
//!
//! ## Predicate
//!
//! When used as a predicate test with `~`, and no arguments are provided, then any valid IP address will pass the predicate test.
//!
//! When used as a predicate test with `~`, with one or many command delimited CIDR forms, then any valid IP address must be within the specified set of CIDR patterns for the predicate to pass.
//!
//! Pattern forms may be based on IPv4 or IPv6.
//!
//! ## Extraction
//!
//! When used as an extraction operation with `~=` the predicate test must pass for successful extraction.  If the predicate test succeeds, then the the prefix and network mask for each CIDR is provided as a key/value record with the original pattern form as key.
//!
//! ## Examples
//!
//! ```tremor
//! match { "meta": "192.168.1.1"} of
//!   case rp = %{ meta ~= cidr|| } => rp
//!   default => "no match"
//! end;
//!
//! ```
//!
//! This will output:
//!
//! ```bash
//! "meta":{
//!   "prefix":[ 192, 168, 1, 1 ],
//!   "mask": [ 255, 255, 255, 255 ]
//!  }
//! ```
//!
//! Cidr also supports filtering the IP if it is within the CIDR specified. This errors out if the IP Address specified isn't in the range of the CIDR. Otherwise, it will return the prefix & mask as above.
//!
//! ```tremor
//! match { "meta": "10.22.0.254" } of
//!   case rp = %{ meta ~= cidr|10.22.0.0/24| } => rp
//!   default => "no match"
//! end;
//! ```
//!
//! This will output:
//!
//! ```bash
//! "meta":{
//!   "prefix":[ 10, 22, 0, 254 ],
//!   "mask": [ 255, 255, 255, 255 ]
//!  }
//! ```
//!
//! The extractor also supports multiple comma-separated ranges. This will return the prefix and mask if it belongs to any one of the CIDRs specified:
//!
//! ```tremor
//! match { "meta": "10.22.0.254" } of
//!   case rp = %{ meta ~= cidr|10.22.0.0/24, 10.21.1.0/24| } => rp
//!   default => "no match"
//! end;
//! ```
//!
//! This will output:
//!
//! ```bash
//! "meta":{
//!   "prefix":[ 10, 22, 0, 254 ],
//!   "mask": [ 255, 255, 255, 255 ]
//!  }
//! ```
//!
//! In case it doesn't belong to the CIDR:
//!
//! ```tremor
//! match { "meta": "19.22.0.254" } of
//!   case rp = %{ meta ~= cidr|10.22.0.0/24, 10.21.1.0/24| } => rp
//!   default => "no match"
//! end;
//! ## Output: "no match"
//! ```

use super::{Error, Result};
use beef::Cow;
use cidr_utils::{
    cidr::{IpCidr, Ipv4Cidr},
    utils::IpCidrCombiner,
};
use halfbrown::{hashmap, HashMap};
use std::{
    hash::BuildHasherDefault,
    iter::{Iterator, Peekable},
    net::{IpAddr, Ipv4Addr},
    result::Result as StdResult,
    slice::Iter,
    str::FromStr,
};
use tremor_value::{Object, Value};

fn parse_network(address: Ipv4Addr, mut itr: Peekable<Iter<u8>>) -> Option<IpCidr> {
    let mut network_length = match itr.next()? {
        c if *c >= b'0' && *c <= b'9' => *c - b'0',
        _ => return None,
    };
    network_length = match itr.next() {
        Some(c) if *c >= b'0' && *c <= b'9' => network_length * 10 + *c - b'0',
        None => network_length,
        Some(_) => return None,
    };
    if network_length > 32 {
        None
    } else {
        Some(IpCidr::V4(
            Ipv4Cidr::from_prefix_and_bits(address, network_length).ok()?,
        ))
    }
}

fn parse_ipv4_fast(ipstr: &str) -> Option<IpCidr> {
    let mut itr = ipstr.as_bytes().iter().peekable();
    //// A
    let mut a: u8 = 0;
    while let Some(c) = itr.next() {
        match *c {
            b'0'..=b'9' => {
                a = if let Some(a) = a.checked_mul(10).and_then(|a| a.checked_add(c - b'0')) {
                    a
                } else {
                    return parse_ipv6_fast(ipstr);
                };
            }
            b'a'..=b'f' | b'A'..=b'F' => return parse_ipv6_fast(ipstr),
            b'/' => return parse_network(Ipv4Addr::new(0, 0, 0, a), itr),
            b'.' => {
                itr.peek()?;
                break;
            }
            _ => return None,
        }
    }
    if itr.peek().is_none() {
        return Some(IpCidr::V4(
            Ipv4Cidr::from_prefix_and_bits(Ipv4Addr::new(0, 0, 0, a), 32).ok()?,
        ));
    };

    //// B
    let mut b: u8 = 0;
    while let Some(e) = itr.next() {
        match *e {
            b'0'..=b'9' => b = b.checked_mul(10).and_then(|b| b.checked_add(e - b'0'))?,
            b'/' => return parse_network(Ipv4Addr::new(a, 0, 0, b), itr),
            b'.' => {
                itr.peek()?;
                break;
            }
            _ => return None,
        }
    }
    if itr.peek().is_none() {
        return Some(IpCidr::V4(
            Ipv4Cidr::from_prefix_and_bits(Ipv4Addr::new(a, 0, 0, b), 32).ok()?,
        ));
    };

    //// C
    let mut c: u8 = 0;
    while let Some(e) = itr.next() {
        match *e {
            b'0'..=b'9' => c = c.checked_mul(10).and_then(|c| c.checked_add(e - b'0'))?,
            b'/' => return parse_network(Ipv4Addr::new(a, b, 0, c), itr),
            b'.' => {
                itr.peek()?;
                break;
            }
            _ => return None,
        }
    }
    if itr.peek().is_none() {
        return Some(IpCidr::V4(
            Ipv4Cidr::from_prefix_and_bits(Ipv4Addr::new(a, b, 0, c), 32).ok()?,
        ));
    };

    //// D
    let mut d: u8 = 0;
    while let Some(e) = itr.next() {
        match *e {
            b'0'..=b'9' => d = d.checked_mul(10).and_then(|d| d.checked_add(e - b'0'))?,
            b'/' => return parse_network(Ipv4Addr::new(a, b, c, d), itr),
            _ => return None,
        }
    }
    let address = Ipv4Addr::new(a, b, c, d);
    Some(IpCidr::V4(
        Ipv4Cidr::from_prefix_and_bits(address, 32).ok()?,
    ))
}

fn parse_ipv6_fast(s: &str) -> Option<IpCidr> {
    IpCidr::from_str(s).ok()
}

#[derive(Debug)]
pub struct Cidr(pub IpCidr);

impl Cidr {
    pub fn from_str(s: &str) -> StdResult<Self, Error> {
        if let Some(cidr) = parse_ipv4_fast(s) {
            Ok(Self(cidr))
        } else {
            Err(Error {
                msg: format!("Invalid CIDR: '{}'", s),
            })
        }
    }
}

impl std::ops::Deref for Cidr {
    type Target = IpCidr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'cidr> From<Cidr>
    for HashMap<Cow<'cidr, str>, Value<'cidr>, BuildHasherDefault<fxhash::FxHasher>>
{
    fn from(x: Cidr) -> Self {
        match x.0 {
            IpCidr::V4(y) => hashmap!(
                       "prefix".into() => Value::from(y.get_prefix_as_u8_array().to_vec()),
                       "mask".into() => Value::from(y.get_mask_as_u8_array().to_vec()),
            ),
            IpCidr::V6(y) => hashmap!(
                       "prefix".into() => Value::from(y.get_prefix_as_u16_array().to_vec()),
                       "mask".into() => Value::from(y.get_mask_as_u16_array().to_vec()),
            ),
        }
    }
}

pub(crate) fn execute(
    s: &str,
    result_needed: bool,
    range: Option<&SnotCombiner>,
) -> Result<'static> {
    if let Some(combiner) = range {
        IpAddr::from_str(s).map_or(Result::NoMatch, |input| {
            if combiner.combiner.contains(input) {
                if result_needed {
                    Cidr::from_str(s).map_or(Result::NoMatch, |cidr| {
                        Result::Match(Value::from(Object::from(cidr)))
                    })
                } else {
                    Result::MatchNull
                }
            } else {
                Result::NoMatch
            }
        })
    } else {
        Cidr::from_str(s).map_or(Result::NoMatch, |c| {
            if result_needed {
                Result::Match(Value::from(Object::from(c)))
            } else {
                Result::MatchNull
            }
        })
    }
}

#[derive(Debug, Serialize)]
pub struct SnotCombiner {
    rules: Vec<String>,
    #[serde(skip)]
    combiner: IpCidrCombiner,
}

impl SnotCombiner {
    pub(crate) fn from_rules(rules: Vec<String>) -> StdResult<Self, Error> {
        let mut combiner = IpCidrCombiner::new();
        for x in &rules {
            if let Some(y) = parse_ipv4_fast(x) {
                combiner.push(y);
            } else {
                return Err(Error {
                    msg: format!("could not parse CIDR: '{}'", x),
                });
            }
        }
        Ok(Self { rules, combiner })
    }
}

impl PartialEq for SnotCombiner {
    fn eq(&self, other: &Self) -> bool {
        self.rules == other.rules
    }
}

impl Clone for SnotCombiner {
    fn clone(&self) -> Self {
        if let Ok(clone) = Self::from_rules(self.rules.clone()) {
            clone
        } else {
            Self {
                combiner: IpCidrCombiner::new(),
                rules: vec![],
            }
        }
    }
}

#[test]
fn test_parse_ipv4_fast() {
    assert_eq!(parse_ipv4_fast("snot"), None);

    assert_eq!(parse_ipv4_fast("0no"), None);
    assert_eq!(parse_ipv4_fast("0."), None);
    assert_eq!(parse_ipv4_fast("0"), IpCidr::from_str("0.0.0.0/32").ok());
    assert_eq!(parse_ipv4_fast("0/24"), IpCidr::from_str("0.0.0.0/24").ok());

    assert_eq!(parse_ipv4_fast("1.2no"), None);
    assert_eq!(parse_ipv4_fast("1.2."), None);
    assert_eq!(parse_ipv4_fast("1.2"), IpCidr::from_str("1.0.0.2/32").ok());
    assert_eq!(
        parse_ipv4_fast("1.2/24"),
        IpCidr::from_str("1.0.0.2/24").ok()
    );

    assert_eq!(parse_ipv4_fast("1.2.3no"), None);
    assert_eq!(parse_ipv4_fast("1.2.3."), None);
    assert_eq!(
        parse_ipv4_fast("1.2.3"),
        IpCidr::from_str("1.2.0.3/32").ok()
    );
    assert_eq!(
        parse_ipv4_fast("1.2.3/24"),
        IpCidr::from_str("1.2.0.3/24").ok()
    );
    assert_eq!(parse_ipv4_fast("1.2.3.4no"), None);
    assert_eq!(parse_ipv4_fast("1.2.3.4."), None);
    assert_eq!(
        parse_ipv4_fast("1.2.3.4"),
        IpCidr::from_str("1.2.3.4/32").ok()
    );
    assert_eq!(
        parse_ipv4_fast("1.2.3.4/24"),
        IpCidr::from_str("1.2.3.4/24").ok()
    );
}
