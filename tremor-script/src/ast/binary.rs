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

use super::raw::{BytesDataType, Endian};
use crate::errors::{err_generic, Result};
use crate::prelude::*;
use crate::stry;
use byteorder::{BigEndian, ByteOrder};

// We are truncating for so we can write parts of the values
#[allow(clippy::cast_possible_truncation)]
fn write_bits_be(
    bytes: &mut Vec<u8>,
    bits: u8,
    buf: &mut u8,
    pending: &mut u8,
    v: u64,
) -> Result<()> {
    // Make sure we don't  have any of the more significant bits set that are not inside of 'bits'
    let bit_mask: u64 = ((1_u128 << (bits + 1)) - 1) as u64;
    let v = v & bit_mask;

    if bits == 0 {
        // Short circuit if we got nothing to write
        Ok(())
    } else if *pending > 0 {
        // we have some pending bits so we got to steal some from the current value
        if bits + *pending >= 8 {
            // we got enough bits to write the buffer
            // calculate how many bits are missing
            let missing = 8 - *pending;
            let shift = bits - missing;
            // use the most significant out all the least significant bits
            // we don't need
            *buf |= (v >> shift) as u8;
            // Pus the updated value
            bytes.push(*buf);
            // reset our buffer
            *pending = 0;
            *buf = 0;
            // write the rest
            write_bits_be(bytes, bits - missing, buf, pending, v)
        } else {
            // we don't got enough bits
            let shift = 8 - (bits + *pending);
            *pending += bits;
            *buf |= (v as u8) << shift;
            Ok(())
        }
    } else if bits % 8 > 0 {
        // our data isn't 8 bit aligned so we chop of the extra bits at the end
        // and then write
        let extra = bits % 8;
        stry!(write_bits_be(bytes, bits - extra, buf, pending, v >> extra));
        let mask = (1_u8 << extra) - 1;
        *buf = (v as u8) & mask;
        *buf <<= 8 - extra;
        *pending = extra;
        Ok(())
    } else {
        match bits {
            8 => bytes.push(v as u8),
            16 => {
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0]);
                // ALLOW: we know bytes is at least `l` long
                BigEndian::write_u16(&mut bytes[l..], v as u16);
            }
            24 => {
                // write the higher 8 bytes first
                bytes.push((v >> 16) as u8);
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0]);
                // ALLOW: we know bytes is at least `l` long
                BigEndian::write_u16(&mut bytes[l..], v as u16);
            }
            32 => {
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0, 0, 0]);
                // ALLOW: we know bytes is at least `l` long
                BigEndian::write_u32(&mut bytes[l..], v as u32);
            }
            40 => {
                bytes.push((v >> 32) as u8);
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0, 0, 0]);
                // ALLOW: we know bytes is at least `l` long
                BigEndian::write_u32(&mut bytes[l..], v as u32);
            }
            48 => {
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
                // ALLOW: we know bytes is at least `l` long
                BigEndian::write_u16(&mut bytes[l..], (v >> 32) as u16);
                // ALLOW: we know bytes is at least `l` long
                BigEndian::write_u32(&mut bytes[l + 2..], v as u32);
            }
            56 => {
                bytes.push((v >> 48) as u8);
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
                // ALLOW: we know bytes is at least `l` long
                BigEndian::write_u16(&mut bytes[l..], (v >> 32) as u16);
                // ALLOW: we know bytes is at least `l` long
                BigEndian::write_u32(&mut bytes[l + 2..], v as u32);
            }
            _ => {
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
                // ALLOW: we know bytes is at least `l` long
                BigEndian::write_u64(&mut bytes[l..], v);
            }
        }
        Ok(())
    }
}

// We allow truncation since we want to cut down the size of values
#[allow(clippy::cast_possible_truncation)]
fn write_bits_le(
    bytes: &mut Vec<u8>,
    bits: u8,
    buf: &mut u8,
    pending: &mut u8,
    v: u64,
) -> Result<()> {
    let bit_mask: u64 = ((1_u128 << (bits + 1)) - 1) as u64;
    let v = v & bit_mask;

    // We write little endian by transforming the number of bits we want to write  into big endian
    // and then write that.
    if bits <= 8 {
        write_bits_be(bytes, bits, buf, pending, v)
    } else if bits <= 16 {
        write_bits_be(bytes, bits, buf, pending, u64::from((v as u16).to_be()))
    } else if bits <= 24 {
        write_bits_be(
            bytes,
            bits,
            buf,
            pending,
            u64::from((v as u32).to_be() >> 8),
        )
    } else if bits <= 32 {
        write_bits_be(bytes, bits, buf, pending, u64::from((v as u32).to_be()))
    } else if bits <= 40 {
        write_bits_be(bytes, bits, buf, pending, v.to_be() >> 24)
    } else if bits <= 48 {
        write_bits_be(bytes, bits, buf, pending, v.to_be() >> 16)
    } else if bits <= 56 {
        write_bits_be(bytes, bits, buf, pending, v.to_be() >> 8)
    } else {
        write_bits_be(bytes, bits, buf, pending, v.to_be())
    }
}

fn write_bits(
    bytes: &mut Vec<u8>,
    bits: u8,
    endianess: Endian,
    buf: &mut u8,
    pending: &mut u8,
    v: u64,
) -> Result<()> {
    match endianess {
        Endian::Big => write_bits_be(bytes, bits, buf, pending, v),
        Endian::Little => write_bits_le(bytes, bits, buf, pending, v),
    }
}

// We allow this so we can cast the bits to u8, this is safe since we limit
// bits to 64 during creation.
// We allow cast sign loss since we translate everything
// into u64 (since we only)
#[allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_arguments
)]
pub(crate) fn extend_bytes_from_value<O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    data_type: BytesDataType,
    endianess: Endian,
    bits: u64,
    buf: &mut u8,
    pending: &mut u8,
    bytes: &mut Vec<u8>,
    value: &Value,
) -> Result<()> {
    let err =
        |e: &str, v: &Value| -> Result<()> { err_generic(outer, inner, &format!("{e}: {v}")) };

    match data_type {
        BytesDataType::UnsignedInteger => value.as_u64().map_or_else(
            || err("Not an unsigned integer", value),
            |v| write_bits(bytes, bits as u8, endianess, buf, pending, v),
        ),
        BytesDataType::SignedInteger => value.as_i64().map_or_else(
            || err("Not an signed integer", value),
            |v| write_bits(bytes, bits as u8, endianess, buf, pending, v as u64),
        ),
        BytesDataType::Binary => {
            if let Some(b) = value.as_bytes().and_then(|b| {
                if bits == 0 {
                    Some(b)
                } else {
                    b.get(..(bits as usize))
                }
            }) {
                if *pending == 0 {
                    bytes.extend_from_slice(b);
                } else {
                    for v in b {
                        stry!(write_bits(bytes, 8, endianess, buf, pending, *v as u64));
                    }
                }
                Ok(())
            } else {
                err("Not a long enough binary", value)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;
    use crate::{registry, Script};

    fn eval_binary(src: &str) -> Vec<u8> {
        let reg: Registry = registry::registry();
        let script = Script::parse(src, &reg).expect("failed to compile test script");

        let mut event = Value::object();
        let mut meta = Value::object();
        let mut state = Value::null();
        let value = script
            .run(
                &EventContext::new(0, None),
                AggrType::Emit,
                &mut event,
                &mut state,
                &mut meta,
            )
            .expect("failed to run test script");
        match value {
            Return::Drop => vec![],
            Return::Emit { value, .. } => value.as_bytes().unwrap_or_default().to_vec(),
            Return::EmitEvent { .. } => event.as_bytes().unwrap_or_default().to_vec(),
        }
    }
    #[test]
    fn test_empty() {
        let empty: [u8; 0] = [];
        assert_eq!(eval_binary("<<>>"), empty);
    }

    #[test]
    fn test_u4() {
        assert_eq!(eval_binary("<< 1:4 >>"), [1]);
        assert_eq!(eval_binary("<< 1:4/little >>"), [1]);
        assert_eq!(eval_binary("<< 1:4/big >>"), [1]);

        assert_eq!(eval_binary("<< 1:4, 1:4 >>"), [17]);
        assert_eq!(eval_binary("<< 1:4/little, 1:4/little >>"), [17]);
        assert_eq!(eval_binary("<< 1:4/big, 1:4/big >>"), [17]);

        assert_eq!(eval_binary("<< 1:2, 1:2 >>"), [5]);
        assert_eq!(eval_binary("<< 1:2/little, 1:2/little >>"), [5]);
        assert_eq!(eval_binary("<< 1:2/big, 1:2/big >>"), [5]);
    }

    #[test]
    fn test_u6() {
        assert_eq!(eval_binary("<< 1:6 >>"), [1]);
        assert_eq!(eval_binary("<< 1:6/little >>"), [1]);
        assert_eq!(eval_binary("<< 1:6/big >>"), [1]);

        assert_eq!(eval_binary("<< 1:6, 1:2 >>"), [5]);
        assert_eq!(eval_binary("<< 1:6/little, 1:2/little >>"), [5]);
        assert_eq!(eval_binary("<< 1:6/big, 1:2/big >>"), [5]);

        assert_eq!(eval_binary("<< 1:2, 1:6 >>"), [65]);
        assert_eq!(eval_binary("<< 1:2/little, 1:6/little >>"), [65]);
        assert_eq!(eval_binary("<< 1:2/big, 1:6/big >>"), [65]);
    }

    #[test]
    fn test_u8() {
        assert_eq!(eval_binary("<< 42 >>"), [42]);
        assert_eq!(eval_binary("<< 42/little >>"), [42]);
        assert_eq!(eval_binary("<< 42/big >>"), [42]);
        assert_eq!(eval_binary("<<1,2,3,4>>"), [1, 2, 3, 4]);
    }

    #[test]
    fn test_u16() {
        assert_eq!(eval_binary("<< 258:16 >>"), [1, 2]);
        assert_eq!(eval_binary("<< 258:16/little >>"), [2, 1]);
        assert_eq!(eval_binary("<< 258:16/big >>"), [1, 2]);
    }
    #[test]
    fn test_u24() {
        assert_eq!(eval_binary("<< 66051:24 >>"), [1, 2, 3]);
        assert_eq!(eval_binary("<< 66051:24/little >>"), [3, 2, 1]);
        assert_eq!(eval_binary("<< 66051:24/big >>"), [1, 2, 3]);
    }

    #[test]
    fn test_u32() {
        assert_eq!(eval_binary("<< 16909060:32 >>"), [1, 2, 3, 4]);
        assert_eq!(eval_binary("<< 4328719365:32 >>"), [2, 3, 4, 5]);
        assert_eq!(eval_binary("<< 16909060:32/little >>"), [4, 3, 2, 1]);
        assert_eq!(eval_binary("<< 16909060:32/big >>"), [1, 2, 3, 4]);
    }

    #[test]
    fn test_u40() {
        assert_eq!(eval_binary("<< 4328719365:40 >>"), [1, 2, 3, 4, 5]);
        assert_eq!(eval_binary("<< 4328719365:40/little >>"), [5, 4, 3, 2, 1]);
        assert_eq!(eval_binary("<< 4328719365:40/big >>"), [1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_u48() {
        assert_eq!(eval_binary("<< 1108152157446:48 >>"), [1, 2, 3, 4, 5, 6]);
        assert_eq!(
            eval_binary("<< 1108152157446:48/little >>"),
            [6, 5, 4, 3, 2, 1]
        );
        assert_eq!(
            eval_binary("<< 1108152157446:48/big >>"),
            [1, 2, 3, 4, 5, 6]
        );
    }

    #[test]
    fn test_u56() {
        assert_eq!(
            eval_binary("<< 283686952306183:56 >>"),
            [1, 2, 3, 4, 5, 6, 7]
        );
        assert_eq!(
            eval_binary("<< 283686952306183:56/little >>"),
            [7, 6, 5, 4, 3, 2, 1]
        );
        assert_eq!(
            eval_binary("<< 283686952306183:56/big >>"),
            [1, 2, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn test_u64() {
        assert_eq!(
            eval_binary("<< 72623859790382856:64 >>"),
            [1, 2, 3, 4, 5, 6, 7, 8]
        );
        assert_eq!(
            eval_binary("<< 72623859790382856:64/little >>"),
            [8, 7, 6, 5, 4, 3, 2, 1]
        );
        assert_eq!(
            eval_binary("<< 72623859790382856:64/big >>"),
            [1, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    #[test]
    fn test_binary_split() {
        assert_eq!(
            eval_binary("<< 1:4, << 1,2,3,4 >>/binary >>"),
            [16, 16, 32, 48, 4]
        );
    }
    #[test]
    fn test_binary_split_sized() {
        assert_eq!(
            eval_binary("<< 1:4, << 1,2,3,4 >>:2/binary >>"),
            [16, 16, 2]
        );
    }
}
