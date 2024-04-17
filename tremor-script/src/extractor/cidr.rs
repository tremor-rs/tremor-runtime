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

use super::Result;
// use beef::Cow;
use cidr_utils::{
    cidr::{IpCidr, Ipv4Cidr, Ipv6Cidr},
    combiner::Ipv4CidrCombiner,
};
use halfbrown::HashMap;
use simd_json::ObjectHasher;
use std::net::Ipv4Addr;
use std::result::Result as StdResult;
use std::{iter::Peekable, slice::Iter, str::FromStr};
use tremor_value::{literal, Object, Value};

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
        Some(IpCidr::V4(Ipv4Cidr::new(address, network_length).ok()?))
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
            Ipv4Cidr::new(Ipv4Addr::new(0, 0, 0, a), 32).ok()?,
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
        let ipv4_addr = Ipv4Addr::new(a, 0, 0, b);
        let ipv4_cidr = Ipv4Cidr::new(ipv4_addr, 32).ok();
        return Some(IpCidr::V4(ipv4_cidr?));
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
            Ipv4Cidr::new(Ipv4Addr::new(a, b, 0, c), 32).ok()?,
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

    Some(IpCidr::V4(Ipv4Cidr::new(address, 32).ok()?))
}

fn parse_ipv6_fast(s: &str) -> Option<IpCidr> {
    let ipv6_cidr = Ipv6Cidr::from_str(s).ok()?;

    if ipv6_cidr.network_length() > 128 {
        None
    } else {
        Some(IpCidr::V6(ipv6_cidr))
    }
}

fn cidr_to_value(x: IpCidr) -> Result<'static> {
    let mut r = HashMap::with_capacity_and_hasher(2, ObjectHasher::default());
    match x {
        IpCidr::V4(y) => {
            // prefix
            let prefix = y.first_address().octets();
            let mask = y.mask().octets();
            let mask: Vec<u64> = vec![
                // NOTE TODO Upgrade literal and conversions for a[x] in tremor-value
                mask[0].into(),
                mask[1].into(),
                mask[2].into(),
                mask[3].into(),
            ];
            r.insert_nocheck("prefix".into(), literal!(prefix.to_vec()));
            r.insert_nocheck("mask".into(), literal!(mask));
        }
        IpCidr::V6(y) => {
            let prefix = y.first_address().octets();
            let mut new_prefix: [u16; 8] = [0; 8];
            #[allow(clippy::needless_range_loop)]
            // NOTE I failed at making this idiomatic, thanks clippy!
            for i in 0..8usize {
                let lhs = prefix.get(i * 2);
                let rhs = prefix.get(i * 2 + 1usize);
                new_prefix[i] = match (lhs, rhs) {
                    (Some(lhs), Some(rhs)) => u16::from(*lhs) << 8 | u16::from(*rhs),
                    _otherwise => {
                        return Result::NoMatch;
                    }
                };
            }

            let mask: [u8; 16] = y.mask().octets();
            let mut new_mask: [u16; 8] = [0; 8];
            #[allow(clippy::needless_range_loop)]
            // NOTE I failed at making this idiomatic, thanks clippy!
            for i in 0..8usize {
                let lhs = mask.get(i * 2);
                let rhs = mask.get(i * 2 + 1usize);
                new_mask[i] = match (lhs, rhs) {
                    (Some(lhs), Some(rhs)) => u16::from(*lhs) << 8 | u16::from(*rhs),
                    _otherwise => {
                        return Result::NoMatch;
                    }
                };
            }
            r.insert_nocheck("prefix".into(), literal!(new_prefix.to_vec()));
            r.insert_nocheck("mask".into(), literal!(new_mask.to_vec()));
        }
    }
    Result::Match(Value::from(Object::from(r)))
}

pub(crate) fn execute(
    s: &str,
    result_needed: bool,
    range: Option<&SnotCombiner>,
) -> Result<'static> {
    if let Some(combiner) = range {
        Ipv4Addr::from_str(s).map_or(Result::NoMatch, |input| {
            if combiner.combiner.contains(&input) {
                if result_needed {
                    IpCidr::from_str(s).map_or(Result::NoMatch, cidr_to_value)
                } else {
                    Result::MatchNull
                }
            } else {
                Result::NoMatch
            }
        })
    } else {
        IpCidr::from_str(s).map_or(Result::NoMatch, |c| {
            if result_needed {
                cidr_to_value(c)
            } else {
                Result::MatchNull
            }
        })
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct SnotCombiner {
    rules: Vec<String>,
    #[serde(skip)]
    combiner: Ipv4CidrCombiner,
}

// NOTE We need this for the extractor
impl SnotCombiner {
    pub(crate) fn from_rules(rules: Vec<String>) -> StdResult<Self, Box<dyn std::error::Error>> {
        let mut combiner = Ipv4CidrCombiner::new();
        for x in &rules {
            if let Some(IpCidr::V4(cidr)) = parse_ipv4_fast(x) {
                combiner.push(cidr);
            } else if let Some(IpCidr::V6(_)) = parse_ipv6_fast(x) {
                return Err(format!("IPv6 CIDR not supported: '{x}'").into());
            } else {
                return Err(format!("could not parse CIDR: '{x}'").into());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ipv4_fast() -> std::result::Result<(), Box<dyn std::error::Error>> {
        assert_eq!(parse_ipv4_fast("snot"), None);

        assert_eq!(parse_ipv4_fast("0no"), None);
        assert_eq!(parse_ipv4_fast("0."), None);
        assert_eq!(
            parse_ipv4_fast("0"),
            Some(IpCidr::V4(Ipv4Cidr::from_str("0.0.0.0/32")?))
        );
        assert_eq!(
            parse_ipv4_fast("0/24"),
            Some(IpCidr::V4(Ipv4Cidr::from_str("0.0.0.0/24")?))
        );

        assert_eq!(parse_ipv4_fast("1.2no"), None);
        assert_eq!(parse_ipv4_fast("1.2."), None);
        assert_eq!(
            parse_ipv4_fast("1.2"),
            Some(IpCidr::V4(Ipv4Cidr::from_str("1.0.0.2/32")?))
        );
        assert_eq!(
            parse_ipv4_fast("1.0/16"),
            Some(IpCidr::V4(Ipv4Cidr::from_str("1.0.0.0/16")?))
        );

        assert_eq!(parse_ipv4_fast("1.2.3no"), None);
        assert_eq!(parse_ipv4_fast("1.2.3."), None);
        assert_eq!(
            parse_ipv4_fast("1.2.3"),
            Some(IpCidr::V4(Ipv4Cidr::from_str("1.2.0.3/32")?))
        );
        // assert_eq!(
        //     parse_ipv4_fast("1.2.3/24"),
        //     Some(IpCidr::V4(Ipv4Cidr::from_str("1.2.0.3/24")?))
        // );
        assert_eq!(parse_ipv4_fast("1.2.3.4no"), None);
        assert_eq!(parse_ipv4_fast("1.2.3.4."), None);
        assert_eq!(
            parse_ipv4_fast("1.2.3.4"),
            Some(IpCidr::V4(Ipv4Cidr::from_str("1.2.3.4/32")?))
        );
        // assert_eq!(
        //     parse_ipv4_fast("1.2.3/24"),
        //     Some(IpCidr::V4(Ipv4Cidr::from_str("1.2.3.4/24")?))
        // );

        Ok(())
    }

    #[test]
    fn test_cidr_utils_v0_6_behavior_change() {
        // CIDR utils crate is now stricter w.r.t. network ranges
        // NOTE that this indirectly affects the behavior of the `cidr` extractor and may
        // need some warnings or documentation updates accordingly.
        assert_eq!(
            parse_ipv4_fast("1.2/16"),
            // Some(IpCidr::V4(Ipv4Cidr::from_str("1.0.0.0/16")?))
            None
        );
    }

    #[test]
    fn test_cidr_utils_v0_6_regression2() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // This test shows how 1.2/16 or similar need to update/change to be compatible with the new CIDR utils crate as it is now stricter w.r.t. network ranges.
        assert_eq!(
            parse_ipv4_fast("1.0/16"),
            Some(IpCidr::V4(Ipv4Cidr::from_str("1.0.0.0/16")?))
        );
        assert_eq!(
            parse_ipv4_fast("1.0/24"),
            Some(IpCidr::V4(Ipv4Cidr::from_str("1.0.0.0/24")?))
        );

        Ok(())
    }
}
