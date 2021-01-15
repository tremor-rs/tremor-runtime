use super::{
    raw::{BytesDataType, Endian},
    BaseExpr, NodeMetas,
};
use crate::errors::{error_generic, Result};
use crate::prelude::*;
use crate::{stry, Value};
use byteorder::{BigEndian, ByteOrder};

fn write_bits_be(bytes: &mut Vec<u8>, bits: u8, buf: &mut u8, used: &mut u8, v: u64) -> Result<()> {
    // Make sure we don't  have any of the more significant bits set that are not inside of 'bits'
    let bit_mask: u64 = ((1u128 << bits + 1) - 1) as u64;
    let v = v & bit_mask;

    if bits == 0 {
        // Short circuit if we got nothing to write
        Ok(())
    } else if *used > 0 {
        // we have some pending bits so we got to steal some from the current value
        if bits + *used >= 8 {
            // we got enough bits to write the buffer
            // calculate how many bits are missing
            let missing = 8 - *used;
            let shift = bits - missing;
            // use the most significant out all the least significant bits
            // we don't need
            *buf |= (v >> shift) as u8;
            // Pus the updated value
            bytes.push(*buf);
            // reset our buffer
            *used = 0;
            *buf = 0;
            // write the rest
            write_bits_be(bytes, bits - missing, buf, used, v)
        } else {
            // we don't got enough bits
            let shift = 8 - (bits + *used);
            *used += bits;
            *buf |= (v as u8) << shift;
            Ok(())
        }
    } else if bits % 8 != 0 {
        // our data isn't 8 bit aligned so we chop of the extra bits at the end
        // and then write
        let extra = bits % 8;
        stry!(write_bits_be(bytes, bits - extra, buf, used, v >> extra));
        let mask = (1u8 << extra) - 1;
        *buf = (v as u8) & mask;
        *buf = *buf << (8 - extra);
        *used = extra;
        Ok(())
    } else {
        match bits {
            8 => bytes.push(v as u8),
            16 => {
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0]);
                BigEndian::write_u16(&mut bytes[l..], v as u16)
            }
            24 => {
                // write the higher 8 bytes first
                bytes.push((v >> 16) as u8);
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0]);
                BigEndian::write_u16(&mut bytes[l..], v as u16)
            }
            32 => {
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0, 0, 0]);
                BigEndian::write_u32(&mut bytes[l..], v as u32)
            }
            40 => {
                bytes.push((v >> 32) as u8);
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0, 0, 0]);
                BigEndian::write_u32(&mut bytes[l..], v as u32)
            }
            48 => {
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
                BigEndian::write_u16(&mut bytes[l..], (v >> 32) as u16);
                BigEndian::write_u32(&mut bytes[l + 2..], v as u32);
            }
            56 => {
                bytes.push((v >> 48) as u8);
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
                BigEndian::write_u16(&mut bytes[l..], (v >> 32) as u16);
                BigEndian::write_u32(&mut bytes[l + 2..], v as u32);
            }
            64 => {
                let l = bytes.len();
                bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
                BigEndian::write_u64(&mut bytes[l..], v)
            }
            _ => {
                unreachable!()
            }
        }
        Ok(())
    }
}

fn write_bits_le(bytes: &mut Vec<u8>, bits: u8, buf: &mut u8, used: &mut u8, v: u64) -> Result<()> {
    let bit_mask: u64 = ((1u128 << bits + 1) - 1) as u64;
    let v = v & bit_mask;

    // We write little endian by transforming the number of bits we want to write  into big endian
    // and then write that.
    if bits <= 8 {
        write_bits_be(bytes, bits, buf, used, v)
    } else if bits <= 16 {
        write_bits_be(bytes, bits, buf, used, (v as u16).to_be() as u64)
    } else if bits <= 32 {
        write_bits_be(bytes, bits, buf, used, (v as u32).to_be() as u64)
    } else {
        write_bits_be(bytes, bits, buf, used, v.to_be())
    }
}

fn write_bits(
    bytes: &mut Vec<u8>,
    bits: u8,
    endianess: Endian,
    buf: &mut u8,
    used: &mut u8,
    v: u64,
) -> Result<()> {
    match endianess {
        Endian::Big => write_bits_be(bytes, bits, buf, used, v),
        Endian::Little => write_bits_le(bytes, bits, buf, used, v),
    }
}

pub(crate) fn extend_bytes_from_value<'value, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    meta: &NodeMetas,
    data_type: BytesDataType,
    endianess: Endian,
    bits: u8,
    buf: &mut u8,
    used: &mut u8,
    bytes: &mut Vec<u8>,
    value: &Value<'value>,
) -> Result<()> {
    let err = |e: &str, v: &Value| -> Result<()> {
        error_generic(outer, inner, &format!("{}: {}", e, v), meta)
    };

    match data_type {
        BytesDataType::UnsignedInteger => {
            if let Some(v) = value.as_u64() {
                write_bits(bytes, bits, endianess, buf, used, v as u64)
            } else {
                err("Not an unsigned integer", &value)
            }
        }
        BytesDataType::SignedInteger => {
            if let Some(v) = value.as_i64() {
                write_bits(bytes, bits, endianess, buf, used, v as u64)
            } else {
                err("Not an unsigned integer", &value)
            }
        }
        BytesDataType::Binary => {
            if let Some(b) = value.as_bytes() {
                bytes.extend_from_slice(&b);
                Ok(())
            } else {
                err("Not an unsigned integer", &value)
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
        // let aggr_reg: AggrRegistry = registry::aggr_registry();
        let script = Script::parse(&crate::path::load(), "<eval>", src.to_string(), &reg)
            .expect("failed to compile test script");

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
    fn test_u8() {
        assert_eq!(eval_binary("<< 42 >>"), [42]);
        assert_eq!(eval_binary("<< 42/little >>"), [42]);
        assert_eq!(eval_binary("<< 42/big >>"), [42]);
    }

    #[test]
    fn test_u4() {
        assert_eq!(eval_binary("<< 1:4 >>"), [1]);
        assert_eq!(eval_binary("<< 1:4/little >>"), [1]);
        assert_eq!(eval_binary("<< 1:4/big >>"), [1]);

        assert_eq!(eval_binary("<< 1:4, 1:4 >>"), [17]);
        assert_eq!(eval_binary("<< 1:4/little, 1:4/little >>"), [17]);
        assert_eq!(eval_binary("<< 1:4/big, 1:4/big >>"), [17]);
    }

    #[test]
    fn test_u16() {
        assert_eq!(eval_binary("<< 42:16 >>"), [00, 42]);
        assert_eq!(eval_binary("<< 42:16/little >>"), [42, 00]);
        assert_eq!(eval_binary("<< 42:16/big >>"), [00, 42]);
    }
}
