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

// This is mostly taken from json-rust's codegen
// as it seems to perform well and it makes snense to see
// if we can adopt the approach
//
// https://github.com/maciejhirsz/json-rust/blob/master/src/codegen.rs

use super::{Object, Value};
use simd_json::{prelude::*, stry, StaticNode};
use std::io::{self, Write};
use tremor_common::base64::BASE64;
use value_trait::generator::{
    BaseGenerator, DumpGenerator, PrettyGenerator, PrettyWriterGenerator, WriterGenerator,
};

//use util::print_dec;

impl<'value> Writable for Value<'value> {
    #[inline]
    fn encode(&self) -> String {
        let mut g = DumpGenerator::new();
        std::mem::drop(g.write_json(self));
        g.consume()
    }

    #[inline]
    fn encode_pp(&self) -> String {
        let mut g = PrettyGenerator::new(2);
        std::mem::drop(g.write_json(self));
        g.consume()
    }

    #[inline]
    fn write<'writer, W>(&self, w: &mut W) -> io::Result<()>
    where
        W: 'writer + Write,
    {
        let mut g = WriterGenerator::new(w);
        g.write_json(self)
    }

    #[inline]
    fn write_pp<'writer, W>(&self, w: &mut W) -> io::Result<()>
    where
        W: 'writer + Write,
    {
        let mut g = PrettyWriterGenerator::new(w, 2);
        g.write_json(self)
    }
}

trait Generator: BaseGenerator {
    type T: Write;

    #[inline(always)]
    fn write_object(&mut self, object: &Object) -> io::Result<()> {
        if object.is_empty() {
            self.write(b"{}")
        } else {
            let mut iter = object.iter();
            stry!(self.write(b"{"));

            // We know this exists since it's not empty
            let Some((key, value)) = iter.next() else {
                // ALLOW: We check against size
                unreachable!()
            };
            self.indent();
            stry!(self.new_line());
            stry!(self.write_simple_string(key));
            stry!(self.write_min(b": ", b':'));
            stry!(self.write_json(value));

            for (key, value) in iter {
                stry!(self.write(b","));
                stry!(self.new_line());
                stry!(self.write_simple_string(key));
                stry!(self.write_min(b": ", b':'));
                stry!(self.write_json(value));
            }
            self.dedent();
            stry!(self.new_line());
            self.write(b"}")
        }
    }

    #[inline(always)]
    fn write_json(&mut self, json: &Value) -> io::Result<()> {
        match *json {
            Value::Static(StaticNode::Null) => self.write(b"null"),
            Value::Static(StaticNode::I64(number)) => self.write_int(number),
            #[cfg(feature = "128bit")]
            Value::Static(StaticNode::I128(number)) => self.write_int(number),
            Value::Static(StaticNode::U64(number)) => self.write_int(number),
            #[cfg(feature = "128bit")]
            Value::Static(StaticNode::U128(number)) => self.write_int(number),
            Value::Static(StaticNode::F64(number)) => self.write_float(number),
            Value::Static(StaticNode::Bool(true)) => self.write(b"true"),
            Value::Static(StaticNode::Bool(false)) => self.write(b"false"),
            Value::String(ref string) => self.write_string(string),
            Value::Array(ref array) => {
                if array.is_empty() {
                    self.write(b"[]")
                } else {
                    let mut iter = <[Value]>::iter(array);
                    // We know we have one item

                    let Some(item) = iter.next() else {
                        // ALLOW: We check against size
                        unreachable!()
                    };
                    stry!(self.write(b"["));
                    self.indent();

                    stry!(self.new_line());
                    stry!(self.write_json(item));

                    for item in iter {
                        stry!(self.write(b","));
                        stry!(self.new_line());
                        stry!(self.write_json(item));
                    }
                    self.dedent();
                    stry!(self.new_line());
                    self.write(b"]")
                }
            }
            Value::Object(ref object) => self.write_object(object),
            Value::Bytes(ref b) => {
                stry!(self.write(b"\""));
                {
                    let mut enc = base64::write::EncoderWriter::new(self.get_writer(), &BASE64);
                    stry!(enc.write_all(b));
                    stry!(enc.finish().map(|_| ()));
                }
                self.write(b"\"")
            }
        }
    }
}

trait FastGenerator: BaseGenerator {
    type T: Write;

    #[inline(always)]
    fn write_object(&mut self, object: &Object) -> io::Result<()> {
        if object.is_empty() {
            self.write(b"{}")
        } else {
            let mut iter = object.iter();
            stry!(self.write(b"{\""));

            // We know this exists since it's not empty
            let Some((key, value)) = iter.next() else {
                // ALLOW: We check against size
                unreachable!()
            };
            stry!(self.write_simple_str_content(key));
            stry!(self.write(b"\":"));
            stry!(self.write_json(value));

            for (key, value) in iter {
                stry!(self.write(b",\""));
                stry!(self.write_simple_str_content(key));
                stry!(self.write(b"\":"));
                stry!(self.write_json(value));
            }
            self.write(b"}")
        }
    }

    #[inline(always)]
    fn write_json(&mut self, json: &Value) -> io::Result<()> {
        match *json {
            Value::Static(StaticNode::Null) => self.write(b"null"),
            Value::Static(StaticNode::I64(number)) => self.write_int(number),
            #[cfg(feature = "128bit")]
            Value::Static(StaticNode::I128(number)) => self.write_int(number),
            Value::Static(StaticNode::U64(number)) => self.write_int(number),
            #[cfg(feature = "128bit")]
            Value::Static(StaticNode::U128(number)) => self.write_int(number),
            Value::Static(StaticNode::F64(number)) => self.write_float(number),
            Value::Static(StaticNode::Bool(true)) => self.write(b"true"),
            Value::Static(StaticNode::Bool(false)) => self.write(b"false"),
            Value::String(ref string) => self.write_string(string),
            Value::Array(ref array) => {
                if array.is_empty() {
                    self.write(b"[]")
                } else {
                    let mut iter = <[Value]>::iter(array);
                    // We know we have one item
                    let Some(item) = iter.next() else {
                        // ALLOW: We check against size
                        unreachable!()
                    };

                    stry!(self.write(b"["));
                    stry!(self.write_json(item));

                    for item in iter {
                        stry!(self.write(b","));
                        stry!(self.write_json(item));
                    }
                    self.write(b"]")
                }
            }
            Value::Object(ref object) => self.write_object(object),
            Value::Bytes(ref b) => {
                stry!(self.write(b"\""));
                {
                    let mut enc = base64::write::EncoderWriter::new(self.get_writer(), &BASE64);
                    stry!(enc.write_all(b));
                    stry!(enc.finish().map(|_| ()));
                }
                self.write(b"\"")
            }
        }
    }
}

impl FastGenerator for DumpGenerator {
    type T = Vec<u8>;
}

impl Generator for PrettyGenerator {
    type T = Vec<u8>;
}

impl<'writer, W> FastGenerator for WriterGenerator<'writer, W>
where
    W: Write,
{
    type T = W;
}

impl<'writer, W> Generator for PrettyWriterGenerator<'writer, W>
where
    W: Write,
{
    type T = W;
}

#[cfg(test)]
mod test {
    use super::Value;
    use simd_json::prelude::*;
    use simd_json::StaticNode;

    #[test]
    fn array() {
        assert_eq!(Value::array().encode(), "[]");
        assert_eq!(Value::from(vec![true]).encode(), "[true]");
        assert_eq!(Value::from(vec![true, false]).encode(), "[true,false]");
    }
    #[test]
    fn array_pp() {
        assert_eq!(Value::array().encode_pp(), "[]");
        assert_eq!(Value::from(vec![true]).encode_pp(), "[\n  true\n]");
        assert_eq!(
            Value::from(vec![true, false]).encode_pp(),
            "[\n  true,\n  false\n]"
        );
    }
    #[test]
    fn null() {
        assert_eq!(Value::Static(StaticNode::Null).encode(), "null");
    }
    #[test]
    fn bool_true() {
        assert_eq!(Value::Static(StaticNode::Bool(true)).encode(), "true");
    }
    #[test]
    fn bool_false() {
        assert_eq!(Value::Static(StaticNode::Bool(false)).encode(), "false");
    }

    #[test]
    fn obj() {
        let mut o = Value::object();
        assert_eq!(o.encode(), "{}");
        o.try_insert("snot", "badger");
        assert_eq!(o.encode(), r#"{"snot":"badger"}"#);
        o.try_insert("badger", "snot");
        assert_eq!(o.encode(), r#"{"snot":"badger","badger":"snot"}"#);
    }
    #[test]
    fn obj_pp() {
        let mut o = Value::object();
        assert_eq!(o.encode_pp(), "{}");
        o.try_insert("snot", "badger");
        assert_eq!(
            o.encode_pp(),
            r#"{
  "snot": "badger"
}"#
        );
        o.try_insert("badger", "snot");
        assert_eq!(
            o.encode_pp(),
            r#"{
  "snot": "badger",
  "badger": "snot"
}"#
        );
    }

    fn assert_str(from: &str, to: &str) {
        assert_eq!(Value::String(from.into()).encode(), to);
    }
    #[test]
    fn string() {
        assert_str(r#"this is a test"#, r#""this is a test""#);
        assert_str(r#"this is a test ""#, r#""this is a test \"""#);
        assert_str(r#"this is a test """#, r#""this is a test \"\"""#);
        assert_str(
            r#"this is a test a long test that should span the 32 byte boundary"#,
            r#""this is a test a long test that should span the 32 byte boundary""#,
        );
        assert_str(
            r#"this is a test a "long" test that should span the 32 byte boundary"#,
            r#""this is a test a \"long\" test that should span the 32 byte boundary""#,
        );

        assert_str(
            r#"this is a test a \"long\" test that should span the 32 byte boundary"#,
            r#""this is a test a \\\"long\\\" test that should span the 32 byte boundary""#,
        );
    }
}
