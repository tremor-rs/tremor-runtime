// Copyright 2018-2020, Wayfair GmbH
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

#![forbid(warnings)]
//! Dissect is a library which is loosely based on logstash's dissect. It extracts data from
//! strings.
//!
//! ```rust
//! use dissect::{Pattern, Error};
//! use simd_json::borrowed::Value;
//!
//! let filter = Pattern::compile("%{a} %{b}")?;
//! let input = "John Doe";
//!
//! let output = filter.run(input).unwrap_or_default();
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("a".into(), Value::from("John"));
//! expected.insert("b".into(), Value::from("Doe"));
//!
//! assert_eq!(output, expected);
//! # Ok::<(), Error>(())
//! ```
//!
//! ### Categories
//!
//! 1) Simple:
//!
//! Named fields can be extracted using the syntax %{<name>} where the given name is then used as a
//! key for the value. The characters between two fields are used as delimiters.
//!
//! ```rust
//! use dissect::{Pattern, Error};
//! use simd_json::borrowed::Value;
//!
//! let output = Pattern::compile("%{name}, %{age}")?;
//! let output = output.run("John Doe, 22").unwrap_or_default();
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".into(), Value::from("John Doe"));
//! expected.insert("age".into(), Value::from("22"));
//!
//! assert_eq!(output, expected);
//! # Ok::<(), Error>(())
//! ```
//!
//! 2) Append (+)
//!
//! The append operator will append the value to another value creating an array.
//!
//! ```rust
//! use dissect::{Pattern, Error};
//! use simd_json::borrowed::Value;
//! let output = Pattern::compile( "%{+name} %{+name}, %{age}")?;
//! let output = output.run("John Doe, 22").unwrap_or_default();
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".into(), Value::from("John Doe"));
//! expected.insert("age".into(), Value::from("22"));
//!
//! assert_eq!(output, expected);
//! # Ok::<(), Error>(())
//! ```
//!
//! Append works only on strings and doesn't support other types. Using append with any non-string
//! type will result in an error.
//!
//! 3) Named keys (&)
//!
//! The named operator will return a key value pair of the field. It takes the key from the
//! previous matched field. Given  the rule, `%{?name}, %{&name}` and input `"John Doe, 22"`,
//! the `"%{?name}"` will match `"John Doe"` but the `?` will prevent this from being stored
//! in the output.
//!
//! The seperator `, ` is skipped and `%{&name}` matches `"22"`. Since the `&` is used, name
//! doesn't become the key but the previous value found for  name `"John Doe"` even so isn't stored
//! in the output, will become the key for `"22"`.
//!
//!
//! ```rust
//! use dissect::{Pattern, Error};
//! use simd_json::borrowed::Value;
//! let output = Pattern::compile("%{?name}, %{&name}")?;
//! let output = output.run( "John Doe, 22").unwrap_or_default();
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("John Doe".into(), Value::from("22"));
//! assert_eq!(output, expected);
//! # Ok::<(), Error>(())
//! ```
//!
//! 4) Empty fields
//!
//! Fields  will return an empty value if no data is present in the input.
//!
//! ```rust
//! use dissect::{Pattern, Error};
//! use simd_json::borrowed::Value;
//!
//! let output = Pattern::compile("%{name}, %{age}")?;
//! let output = output.run(", 22").unwrap_or_default();
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".into(), Value::from(""));
//! expected.insert("age".into(), Value::from("22"));
//! assert_eq!(output, expected);
//! # Ok::<(), Error>(())
//! ```
//!
//! 5) Skipped fields (?)
//!
//! The operator will prevent the value from being stored in the output, effectively skipping it.
//!
//! ```rust
//! use dissect::{Pattern, Error};
//! use simd_json::borrowed::Value;
//! let output = Pattern::compile("%{?first_name} %{last_name}, %{age}")?;
//! let output = output.run("John Doe, 22").unwrap_or_default();
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("last_name".into(), Value::from("Doe"));
//! expected.insert("age".into(), Value::from("22"));
//! assert_eq!(output, expected);
//! # Ok::<(), Error>(())
//! ```
//!
//! 6) Types
//!
//! We can convert the fields in the output to a different type by mentioning it in the field
//! definition. The types supported are: int, float, string. The type is specified with the
//! `field : type` syntax.
//!
//! ```rust
//!
//! use dissect::{Pattern, Error};
//! use simd_json::borrowed::Value;
//! let output = Pattern::compile("%{name}, %{age:int}")?;
//! let output = output.run( "John Doe, 22").unwrap_or_default();
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".into(), Value::from("John Doe"));
//! expected.insert("age".into(),Value::from(22));
//! assert_eq!(output, expected);
//! # Ok::<(), Error>(())
//! ```
//!
//! 7) Padding (_)
//!
//! The operator will remove padding when storing the field in the output. You can specify the
//! skipped character as a parameter to `_`. It will use ` ` by default.
//!
//! ```rust
//! use dissect::{Pattern, Error};
//! use simd_json::borrowed::Value;
//! let output = Pattern::compile("%{name}, %{_}%{age}")?;
//! let output = output.run("John Doe,                22").unwrap_or_default();
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".into(), Value::from("John Doe"));
//! expected.insert("age".into(), Value::from("22"));
//! assert_eq!(output, expected);
//! # Ok::<(), Error>(())
//! ```
//!
//! ```rust
//! use dissect::{Pattern, Error};
//! use simd_json::borrowed::Value;
//! let output = Pattern::compile("%{name}, %{_(-)}%{age}")?;
//! let output = output.run("John Doe, -----------------------22").unwrap_or_default();
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".into(), Value::from("John Doe"));
//! expected.insert("age".into(), Value::from("22"));
//! assert_eq!(output, expected);
//! # Ok::<(), Error>(())
//! ```

#![forbid(warnings)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::result_unwrap_used,
    clippy::option_unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(clippy::must_use_candidate)]

use halfbrown::HashMap;
use simd_json::value::borrowed::{Object, Value};
use std::fmt;

#[derive(PartialEq, Debug, Clone, Copy)]
enum ExtractType {
    String,
    Int,
    Float,
}

impl std::default::Default for ExtractType {
    fn default() -> Self {
        Self::String
    }
}

#[derive(PartialEq, Debug, Clone)]
enum Command {
    Delimiter(String),
    Pattern {
        ignore: bool,
        lookup: bool,
        add: bool,
        name: String,
        convert: ExtractType,
    },
    Padding(String),
}

#[derive(PartialEq, Debug, Clone)]
pub enum Error {
    ConnectedExtractors(usize),
    Unterminated(usize),
    PaddingFollowedBySelf(usize),
    InvalidPad(usize),
    InvalidType(usize, String),
    InvalidEscape(char),
    UnterminatedEscape,
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectedExtractors(p) => write!(
                f,
                "A dilimiter needs to be provided between the two patterns at {}",
                p
            ),
            Self::Unterminated(p) => write!(f, "Unterminated patter at {}", p),
            Self::PaddingFollowedBySelf(p) => write!(
                f,
                "The padding at {} can't be followed up by a dilimiter that begins with it",
                p
            ),
            Self::InvalidPad(p) => write!(f, "Invalid padding at {}", p),
            Self::InvalidType(p, t) => write!(f, "Invalid type '{}' at {}", p, t),
            Self::InvalidEscape(s) => write!(f, "Invalid escape sequence \\'{}' is not valid.", s),
            Self::UnterminatedEscape => write!(
                f,
                "Unterminated escape at the end of line or of a delimiter %{{ can't be escaped"
            ),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Pattern {
    commands: Vec<Command>,
}
fn handle_scapes(s: &str) -> Result<String, Error> {
    let mut res = String::with_capacity(s.len());
    let mut cs = s.chars();
    while let Some(c) = cs.next() {
        match c {
            '\\' => {
                if let Some(c1) = cs.next() {
                    match c1 {
                        '\\' => res.push(c1),
                        'n' => res.push('\n'),
                        't' => res.push('\t'),
                        'r' => res.push('\r'),
                        other => return Err(Error::InvalidEscape(other)),
                    }
                } else {
                    return Err(Error::UnterminatedEscape);
                }
            }
            c => res.push(c),
        }
    }
    Ok(res)
}
impl Pattern {
    #[allow(clippy::too_many_lines)]
    pub fn compile(mut pattern: &str) -> Result<Self, Error> {
        fn parse_extractor(mut extractor: &str, idx: usize) -> Result<Command, Error> {
            if extractor.is_empty() {
                return Ok(Command::Pattern {
                    ignore: true,
                    add: false,
                    lookup: false,
                    name: "".to_owned(),
                    convert: ExtractType::String,
                });
            }
            match &extractor[0..1] {
                "?" => Ok(Command::Pattern {
                    ignore: true,
                    add: false,
                    lookup: false,
                    name: extractor[1..].to_owned(),
                    convert: ExtractType::String,
                }),
                "&" => {
                    if let Some(type_pos) = extractor.find(':') {
                        let t = match extractor.get(type_pos + 1..) {
                            Some("int") => ExtractType::Int,
                            Some("float") => ExtractType::Float,
                            Some("string") => ExtractType::String,
                            Some(other) => return Err(Error::InvalidType(idx, other.to_string())),
                            None => return Err(Error::InvalidType(idx, "<EOF>".to_string())),
                        };
                        Ok(Command::Pattern {
                            lookup: true,
                            add: false,
                            ignore: false,
                            name: extractor[1..type_pos].to_owned(),
                            convert: t,
                        })
                    } else {
                        Ok(Command::Pattern {
                            lookup: true,
                            add: false,
                            ignore: false,
                            name: extractor[1..].to_owned(),
                            convert: ExtractType::String,
                        })
                    }
                }
                "+" => Ok(Command::Pattern {
                    add: true,
                    ignore: false,
                    lookup: false,
                    name: extractor[1..].to_owned(),
                    convert: ExtractType::String,
                }),
                "_" => {
                    if extractor.len() == 1 {
                        Ok(Command::Padding(" ".to_owned()))
                    } else {
                        extractor = &extractor[1..];
                        if extractor.starts_with('(') && extractor.ends_with(')') {
                            Ok(Command::Padding(
                                extractor[1..extractor.len() - 1].to_owned(),
                            ))
                        } else {
                            Err(Error::InvalidPad(idx))
                        }
                    }
                }
                _ => {
                    if let Some(type_pos) = extractor.find(':') {
                        let t = match extractor.get(type_pos + 1..) {
                            Some("int") => ExtractType::Int,
                            Some("float") => ExtractType::Float,
                            Some("string") => ExtractType::String,
                            Some(other) => return Err(Error::InvalidType(idx, other.to_string())),
                            None => return Err(Error::InvalidType(idx, "<EOF>".to_string())),
                        };
                        Ok(Command::Pattern {
                            ignore: false,
                            add: false,
                            lookup: false,
                            name: extractor[..type_pos].to_owned(),
                            convert: t,
                        })
                    } else {
                        Ok(Command::Pattern {
                            ignore: false,
                            add: false,
                            lookup: false,
                            name: extractor.to_owned(),
                            convert: ExtractType::String,
                        })
                    }
                }
            }
        }
        let mut commands = Vec::new();
        let mut idx = 0;
        let mut was_extract = false;
        loop {
            if pattern.is_empty() {
                return Ok(Self { commands });
            }
            if pattern.starts_with("%{") {
                if let Some(i) = pattern.find('}') {
                    if let Some(next_open) = pattern[2..].find("%{") {
                        // Have to add 2 because we started searching at pattern + 2
                        if (next_open + 2) < i {
                            return Err(Error::Unterminated(idx));
                        }
                    }
                    let p = parse_extractor(&pattern[2..i], idx)?;
                    // Padding doesn't count as an extractor
                    pattern = &pattern[i + 1..];
                    was_extract = if let Command::Padding(pad) = &p {
                        if pattern.starts_with(pad) {
                            return Err(Error::PaddingFollowedBySelf(idx));
                        };
                        false
                    } else {
                        if was_extract {
                            return Err(Error::ConnectedExtractors(idx));
                        };
                        true
                    };
                    commands.push(p);
                    idx += i + 1;
                } else {
                    return Err(Error::Unterminated(idx));
                }
            } else {
                was_extract = false;
                if let Some(i) = pattern.find("%{") {
                    commands.push(Command::Delimiter(handle_scapes(&pattern[0..i])?));
                    pattern = &pattern[i..];
                    idx += i;
                } else {
                    // No more extractors found
                    commands.push(Command::Delimiter(handle_scapes(pattern)?));
                    return Ok(Self { commands });
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    pub fn run(&self, mut data: &str) -> Option<Object<'static>> {
        #[allow(clippy::too_many_arguments)]
        fn insert(
            r: &mut Object<'static>,
            name: String,
            data: &str,
            add: bool,
            ignored: &mut HashMap<String, String>,
            ignore: bool,
            last_sep: &str,
            convert: ExtractType,
        ) -> Option<()> {
            if ignore {
                ignored.insert(name, data.to_owned());
            } else if add {
                match r.remove(name.as_str()) {
                    None => r.insert(name.into(), Value::from(data.to_owned())),
                    Some(Value::String(s)) => {
                        let mut s = s.to_string();
                        s.push_str(&last_sep);
                        s.push_str(data);
                        r.insert(name.into(), Value::from(s))
                    }
                    Some(_) => None,
                };
            } else {
                let v = match convert {
                    ExtractType::String => Value::from(data.to_owned()),
                    ExtractType::Int => Value::from(data.parse::<i64>().ok()?),
                    ExtractType::Float => Value::from(data.parse::<f64>().ok()?),
                };
                r.insert(name.into(), v);
            }
            Some(())
        }

        let mut r = Object::new();
        let mut ignored: HashMap<String, String> = HashMap::new();
        let mut last_sep = String::from(" ");
        let mut t = 0;
        loop {
            match self.commands.get(t) {
                // No more pattern we're good if no data is left otherwise
                // we do not match
                None => {
                    if data.is_empty() {
                        return Some(r);
                    } else {
                        // We still have data left so it's not a string
                        return None;
                    }
                }
                // We want to skip some text, do so if it's there
                Some(Command::Delimiter(s)) => {
                    if data.starts_with(s) {
                        data = &data[s.len()..];
                    } else {
                        return None;
                    }
                }
                Some(Command::Padding(p)) => {
                    last_sep = p.clone();
                    data = data.trim_start_matches(p);
                }
                // We know a extractor can never be followed by another extractor
                Some(Command::Pattern {
                    ignore,
                    lookup,
                    name,
                    add,
                    convert,
                }) => {
                    let name = if *lookup {
                        match ignored.remove(name) {
                            Some(s) => {
                                if s.is_empty() {
                                    return None;
                                } else {
                                    s
                                }
                            }
                            _ => return None,
                        }
                    } else {
                        name.clone()
                    };
                    match self.commands.get(t + 1) {
                        // This is the last pattern so we eat it all
                        None => {
                            insert(
                                &mut r,
                                name,
                                data,
                                *add,
                                &mut ignored,
                                *ignore,
                                &last_sep,
                                *convert,
                            )?;
                            return Some(r);
                        }
                        Some(Command::Padding(s)) => {
                            if let Some(i) = data.find(s) {
                                insert(
                                    &mut r,
                                    name,
                                    &data[..i],
                                    *add,
                                    &mut ignored,
                                    *ignore,
                                    &last_sep,
                                    *convert,
                                )?;
                                data = &data[i..];
                            } else {
                                // If the padding is the last element we don't need it.
                                match self.commands.get(t + 2) {
                                    None => {
                                        insert(
                                            &mut r,
                                            name,
                                            data,
                                            *add,
                                            &mut ignored,
                                            *ignore,
                                            &last_sep,
                                            *convert,
                                        )?;
                                        data = &data[data.len()..];
                                    }
                                    Some(Command::Delimiter(s)) => {
                                        if let Some(i) = data.find(s) {
                                            insert(
                                                &mut r,
                                                name,
                                                &data[..i],
                                                *add,
                                                &mut ignored,
                                                *ignore,
                                                &last_sep,
                                                *convert,
                                            )?;
                                            data = &data[i..];
                                        } else {
                                            return None;
                                        }
                                    }
                                    _ => {
                                        return None;
                                    }
                                }
                            }
                        }
                        Some(Command::Delimiter(s)) => {
                            if let Some(i) = data.find(s) {
                                insert(
                                    &mut r,
                                    name,
                                    &data[..i],
                                    *add,
                                    &mut ignored,
                                    *ignore,
                                    &last_sep,
                                    *convert,
                                )?;
                                data = &data[i..];
                            } else {
                                return None;
                            }
                        }
                        // We do not allow having two extractors next to each other
                        Some(_) => return None,
                    }
                }
            };
            t += 1;
        }
    }
}

#[cfg(test)]
impl PartialEq<Vec<Command>> for Pattern {
    fn eq(&self, other: &Vec<Command>) -> bool {
        self.commands == *other
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use simd_json::value::borrowed::Value;

    fn cp(pattern: &str) -> Pattern {
        Pattern::compile(pattern).expect("failed to compile pattern")
    }
    fn run(pattern: &str, input: &str) -> Option<Object<'static>> {
        cp(pattern).run(input)
    }
    fn pat(name: &str) -> Command {
        Command::Pattern {
            ignore: false,
            lookup: false,
            add: false,
            name: name.to_string(),
            convert: ExtractType::String,
        }
    }
    fn del(name: &str) -> Command {
        Command::Delimiter(name.to_string())
    }
    fn pad(name: &str) -> Command {
        Command::Padding(name.to_string())
    }

    fn v<'dissect, T: Copy>(s: &'dissect [(&str, T)]) -> Option<Object<'dissect>>
    where
        Value<'dissect>: From<T>,
    {
        use std::borrow::Cow;
        Some(
            s.iter()
                .map(|(x, y)| (Into::<Cow<'dissect, str>>::into(*x), Value::from(*y)))
                .collect(),
        )
    }

    macro_rules! assert_pattern {
        // crate::metrics::INSTANCE is never muated after the initial setting
        // in main::run() so we can use this safely.
        ($pattern:expr, $input:expr) => {
            assert_eq!(run($pattern, $input), None)
        };
        ($pattern:expr, $input:expr, $($args:expr),*) => {
            assert_eq!(run($pattern, $input), v(&[$($args),*]))
        };
    }

    #[test]
    fn empty() {
        assert_eq!(cp(""), vec![]);
    }

    #[test]
    fn parse_stray_closing_backet_is_considered_a_literal() {
        assert_eq!(cp("}"), vec![del("}")]);
    }

    #[test]
    fn parse_literal_percentage() {
        assert_eq!(cp("%{a}%"), vec![pat("a"), del("%")]);
    }

    #[test]
    fn parse_all_edgecases() {
        let testcases = [
            ("%{name}}%{age}", vec![pat("name"), del("}"), pat("age")]),
            ("%{name}%%{age}", vec![pat("name"), del("%"), pat("age")]),
            (".%{name}", vec![del("."), pat("name")]),
            ("foo %{name}", vec![del("foo "), pat("name")]),
            (
                "foo %{name} bar",
                vec![del("foo "), pat("name"), del(" bar")],
            ),
            ("%{name} bar", vec![pat("name"), del(" bar")]),
            ("%{name}bar", vec![pat("name"), del("bar")]),
            ("name%{bar}", vec![del("name"), pat("bar")]),
            (
                "%{name} %{age} %{country}",
                vec![pat("name"), del(" "), pat("age"), del(" "), pat("country")],
            ),
            (
                "%{name} %{age}-%{country}",
                vec![pat("name"), del(" "), pat("age"), del("-"), pat("country")],
            ),
            ("this is %{test}", vec![del("this is "), pat("test")]),
            ("%{test} case", vec![pat("test"), del(" case")]),
            (
                "this is %{test} case",
                vec![del("this is "), pat("test"), del(" case")],
            ),
            (
                "this is %{test} case named %{name}",
                vec![
                    del("this is "),
                    pat("test"),
                    del(" case named "),
                    pat("name"),
                ],
            ),
            (
                "this is %{test} case named %{?name}",
                vec![
                    del("this is "),
                    pat("test"),
                    del(" case named "),
                    Command::Pattern {
                        ignore: true,
                        lookup: false,
                        add: false,
                        name: "name".to_string(),
                        convert: ExtractType::String,
                    },
                ],
            ),
        ];

        for case in &testcases {
            assert_eq!(cp(case.0), case.1);
        }
    }

    #[test]
    fn extract() {
        assert_eq!(cp("%{test}"), vec![pat("test")]);
    }

    #[test]
    fn prefix() {
        assert_eq!(cp("this is %{test}"), vec![del("this is "), pat("test")]);
    }

    #[test]
    fn suffix() {
        assert_eq!(cp("%{test} case"), vec![pat("test"), del(" case"),]);
    }

    #[test]
    fn encircled() {
        assert_eq!(
            cp("this is %{test} case"),
            vec![
                del("this is "),
                Command::Pattern {
                    ignore: false,
                    lookup: false,
                    add: false,
                    convert: ExtractType::String,
                    name: "test".to_owned()
                },
                del(" case"),
            ]
        );
    }

    #[test]
    fn two_patterns() {
        assert_eq!(
            cp("this is %{test} case named %{name}"),
            vec![
                del("this is "),
                pat("test"),
                del(" case named "),
                pat("name"),
            ]
        );
    }

    #[test]
    fn two_ignore() {
        assert_eq!(
            cp("this is %{test} case named %{?name}"),
            vec![
                del("this is "),
                Command::Pattern {
                    ignore: false,
                    lookup: false,
                    add: false,
                    convert: ExtractType::String,
                    name: "test".to_owned()
                },
                del(" case named "),
                Command::Pattern {
                    ignore: true,
                    lookup: false,
                    add: false,
                    convert: ExtractType::String,
                    name: "name".to_owned()
                },
            ]
        );
    }

    #[test]
    fn open_extract() {
        assert_eq!(Pattern::compile("%{test"), Err(Error::Unterminated(0)));
    }

    #[test]
    fn open_middle() {
        assert_eq!(
            Pattern::compile("%{test %{case}"),
            Err(Error::Unterminated(0))
        );
    }

    #[test]
    fn extract_inside() {
        assert_eq!(
            Pattern::compile("this is %{test case"),
            Err(Error::Unterminated(8))
        );
    }

    #[test]
    fn connected_extract() {
        assert_eq!(
            Pattern::compile("%{test}%{pattern}"),
            Err(Error::ConnectedExtractors(7))
        );
    }

    #[test]
    fn dissect_all_edgecases() {
        let testcases = vec![
            (
                "%{name}}%{age}",
                "John}22",
                v(&[("name", "John"), ("age", "22")]),
            ),
            (
                "%{name}%%{age}",
                "John%22",
                v(&[("name", "John"), ("age", "22")]),
            ),
            ("%{name}%%{age}", "John}22", None),
            (".%{name}", ".John", v(&[("name", "John")])),
            (".%{name}", "John", None),
            ("foo %{name}", "foo John", v(&[("name", "John")])),
            ("foo %{name} bar", "foo John bar", v(&[("name", "John")])),
            ("%{name} bar", "John bar", v(&[("name", "John")])),
            ("%{name}bar", "Johnbar", v(&[("name", "John")])),
            ("name%{bar}", "nameJohn", v(&[("bar", "John")])),
            (
                "%{name} %{age} %{country}",
                "John 22 Germany",
                v(&[("name", "John"), ("age", "22"), ("country", "Germany")]),
            ),
            (
                "%{name} %{age}-%{country}",
                "John 22-Germany",
                v(&[("name", "John"), ("age", "22"), ("country", "Germany")]),
            ),
            (
                "this is a %{name} case",
                "this is a John case",
                v(&([("name", "John")])),
            ),
            (
                "this is a %{what} case named %{name}",
                "this is a test case named John",
                v(&([("what", "test"), ("name", "John")])),
            ),
            (
                "this is a %{what}%{_}case named %{name}",
                "this is a test  case named John",
                v(&[("what", "test"), ("name", "John")]),
            ),
            (
                "this is a %{arr} %{+arr}",
                "this is a test case",
                v(&[("arr", "test case")]),
            ),
            // FIXME: Do we want to suppor those?
            // (
            //     "%{name}%{_}%{_(|)}/%{age}",
            //     "John/22",
            //     v(&[("name", "John"), ("age", "22")])),
            // ),
            // (
            //     "%{name}%{_}%{_(|)}/%{age}",
            //     "John|/22",
            //     v(&[("name", "John"), ("age", "22")])),
            // ),
            // (
            //     "%{name}%{_}%{_(|)}/%{age}",
            //     "John /22",
            //     v(&[("name", "John"), ("age", "22")])),
            // ),
            // (
            //     "%{name}%{_}%{_(|)}/ %{age}",
            //     "John|/ 22",
            //     v(&[("name", "John"), ("age", "22")])),
            // ),
            // (
            //     "%{name}%{_}%{_(|)}%{age}",
            //     "John||22",
            //     v(&[("name", "John"), ("age", "22")])),
            // ),
            (
                "%{name}%{_}%{_(|)}%{age}",
                "John 22",
                v(&[("name", "John"), ("age", "22")]),
            ),
            ("%{name} cake", "John cake", v(&[("name", "John")])),
            ("%{name} cake", "John", None),
            ("%{name}%{_}%{_(|)}%{age}", "John22", None),
            (
                "%{a}%{_}%{b}",
                "this    works",
                v(&[("a", "this"), ("b", "works")]),
            ),
            ("%{a}%{_}", "this   ", v(&[("a", "this")])),
            ("%{a}%{_}", "this", v(&[("a", "this")])),
        ];

        for (pattern, input, expected) in testcases {
            assert_eq!(run(dbg!(pattern), dbg!(input)), expected);
        }
    }

    #[test]
    fn dissect_string_with_delimiter_at_the_end_returns_err() {
        let pattern = "%{syslog_timestamp} %{wf_host} %{syslog_program}: %{syslog_message}%{_}";
        let input = "2019-04-26 tremor-host tremor: dissect is working fine";
        assert_pattern!(pattern, input);
    }

    #[test]
    fn dissect_with_optional_padding_in_the_middle() {
        let pattern = "%{name}%{_}|%{age}";
        let input = "John|22";
        assert_pattern!(pattern, input, ("name", "John"), ("age", "22"));
    }

    #[test]
    fn do_extract() {
        let pattern = "this is a %{name} case";
        let input = "this is a test case";
        assert_pattern!(pattern, input, ("name", "test"))
    }

    #[test]
    fn do_extract2() {
        let p = Pattern::compile("this is a %{what} case named %{name}")
            .expect("failed to compile pattern");
        let mut m = Object::new();
        m.insert("what".into(), Value::from("test"));
        m.insert("name".into(), Value::from("cake"));
        assert_eq!(p.run("this is a test case named cake"), Some(m))
    }

    #[test]
    fn do_extract_with_padding() {
        let p = Pattern::compile("this is a %{what}%{_}case named %{name}")
            .expect("failed to compile pattern");
        assert_eq!(
            p,
            vec![
                del("this is a "),
                Command::Pattern {
                    ignore: false,
                    lookup: false,
                    add: false,
                    convert: ExtractType::String,
                    name: "what".to_owned()
                },
                pad(" "),
                del("case named "),
                Command::Pattern {
                    ignore: false,
                    lookup: false,
                    add: false,
                    convert: ExtractType::String,
                    name: "name".to_owned()
                },
            ]
        );
        let mut m = Object::new();
        m.insert("what".into(), Value::from("test"));
        m.insert("name".into(), Value::from("cake"));
        assert_eq!(p.run("this is a test      case named cake"), Some(m))
    }

    #[test]
    fn two_pads() {
        let p = cp("%{_}this%{_}%{_(-)}works");
        assert_eq!(
            p,
            vec![pad(" "), del("this"), pad(" "), pad("-"), del("works"),]
        );
        let m = HashMap::new();
        assert_eq!(p.run("this     -----works"), Some(m))
    }

    #[test]
    fn middle_pads_w_delim() {
        let p = cp("|%{n}%{_}|");
        assert_eq!(
            p,
            vec![
                del("|"),
                Command::Pattern {
                    ignore: false,
                    lookup: false,
                    add: false,
                    convert: ExtractType::String,
                    name: "n".to_owned()
                },
                pad(" "),
                del("|"),
            ]
        );
        let mut m = Object::new();
        m.insert("n".into(), Value::from("Jim"));
        assert_eq!(p.run("|Jim |"), Some(m));
        let mut m = Object::new();
        m.insert("n".into(), Value::from("John"));
        assert_eq!(p.run("|John|"), Some(m));
    }

    #[test]
    fn middle_pads() {
        let p = cp("%{a}%{_}%{b}");
        assert_eq!(p, vec![pat("a"), pad(" "), pat("b"),]);
        let mut m = Object::new();
        m.insert("a".into(), Value::from("this"));
        m.insert("b".into(), Value::from("works"));
        assert_eq!(p.run("this     works"), Some(m))
    }

    #[test]
    fn left_pads() {
        let p = Pattern::compile("%{_}%{b}").expect("failed to compile pattern");
        assert_eq!(p, vec![pad(" "), pat("b"),]);
        let mut m = Object::new();
        m.insert("b".into(), Value::from("works"));
        assert_eq!(p.run("     works"), Some(m))
    }

    #[test]
    fn right_pads() {
        let p = Pattern::compile("%{a}%{_}").expect("failed to compile pattern");
        assert_eq!(p, vec![pat("a"), pad(" ")]);
        let mut m = Object::new();
        m.insert("a".into(), Value::from("this"));
        assert_eq!(p.run("this     "), Some(m))
    }

    #[test]
    fn right_pads_last() {
        let p = Pattern::compile("%{a}%{_}").expect("failed to compile pattern");
        assert_eq!(
            p,
            vec![
                Command::Pattern {
                    ignore: false,
                    lookup: false,
                    add: false,
                    convert: ExtractType::String,
                    name: "a".to_owned()
                },
                pad(" "),
            ]
        );
        let mut m = Object::new();
        m.insert("a".into(), Value::from("this"));
        assert_eq!(p.run("this"), Some(m))
    }

    #[test]
    fn do_extract_with_padding_specific() {
        let p = Pattern::compile("this is a %{what}%{_( case)} named %{name}")
            .expect("failed to compile pattern");
        assert_eq!(
            p,
            vec![
                del("this is a "),
                Command::Pattern {
                    ignore: false,
                    lookup: false,
                    add: false,
                    convert: ExtractType::String,
                    name: "what".to_owned()
                },
                pad(" case"),
                del(" named "),
                Command::Pattern {
                    ignore: false,
                    lookup: false,
                    add: false,
                    convert: ExtractType::String,
                    name: "name".to_owned()
                },
            ]
        );
        let mut m = Object::new();
        m.insert("what".into(), Value::from("test"));
        m.insert("name".into(), Value::from("cake"));
        assert_eq!(p.run("this is a test case named cake"), Some(m))
    }

    #[test]
    fn do_extract_ignore() {
        let p = Pattern::compile("this is a %{?what} case named %{name}")
            .expect("failed to compile pattern");
        let mut m = Object::new();
        m.insert("name".into(), Value::from("cake"));
        assert_eq!(p.run("this is a test case named cake"), Some(m))
    }

    #[test]
    fn do_kv() {
        assert_pattern!(
            "this is a %{?name} case named %{&name}",
            "this is a test case named cake",
            ("test", "cake")
        );
        assert_pattern!(
            "this is a %{?name} %{what} named %{&name}",
            "this is a test case named cake",
            ("what", "case"),
            ("test", "cake")
        );
        assert_pattern!("%{?name}: %{&name:int}", "key: 42", ("key", 42));
    }

    #[test]
    fn do_arr() {
        let p = Pattern::compile("this is a %{+arr} case named %{+arr}")
            .expect("failed to compile pattern");
        let mut m = Object::new();
        m.insert("arr".into(), Value::from("test cake"));
        assert_eq!(p.run("this is a test case named cake"), Some(m))
    }

    #[test]
    fn do_arr_upgrade() {
        let p = Pattern::compile("this is a %{arr} case named %{+arr}")
            .expect("failed to compile pattern");
        let mut m = Object::new();
        m.insert("arr".into(), Value::from("test cake"));
        assert_eq!(p.run("this is a test case named cake"), Some(m))
    }

    #[test]
    fn dissect_all_usecases() {
        let patterns = vec![
                    (
                        "%{syslog_timestamp} %{wf_host} %{syslog_program}: %{syslog_message}%{_}",
                        "12345 host program: message ",
                        v(&([
                            ("syslog_timestamp", "12345"),
                            ("wf_host", "host"),
                            ("syslog_program", "program"),
                            ("syslog_message", "message"),
                        ])),
                    ),
                    (
                        "%{syslog_timestamp} %{wf_host} %{syslog_program}: %{syslog_message}",
                        "12345 host program: message",
                        v(&([
                            ("syslog_timestamp", "12345"),
                            ("wf_host", "host"),
                            ("syslog_program", "program"),
                            ("syslog_message", "message"),
                        ])),
                    ),
                    (
                        "%{}, [%{log_timestamp} #%{pid}] %{log_level} -- %{}: %{message}",
                        "foo, [12345 #12] high -- 1: log failed",
                        v(&([
                            ("log_timestamp", "12345"),
                            ("pid", "12"),
                            ("log_level", "high"),
                            ("message", "log failed"),
                        ])),
                    ),

                    (
                        "%{}>%{+syslog_timestamp} %{+syslog_timestamp} %{+syslog_timestamp} %{syslog_hostname} %{syslog_program}: %{full_message}",
                        "foo>12345 67890 12345 host program: log failed",
                        v(&([
                                 ("syslog_timestamp", "12345 67890 12345"),
                                 ("syslog_hostname", "host"),
                                 ("syslog_program", "program"),
                                 ("full_message", "log failed")
                        ]))
                    ),

                    (

                        "%{syslog_timestamp} %{wf_host} %{}: %{log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{job_name} %{build_num} %{message} completed: %{completed}\n",
                        "12345 host foo: 12345 67890 12345 67890 12345 some_job 12345 some_message completed: 100\n",
                        v(&([
                                 ("syslog_timestamp", "12345"),
                                  ("wf_host", "host"),
                                  ("log_timestamp", "12345 67890 12345 67890 12345"),
                                  ("job_name", "some_job"),
                                  ("build_num", "12345"),
                                  ("message", "some_message"),
                                  ("completed", "100"),
                        ])),
                    ),

                    (

                        "%{syslog_timestamp} %{wf_host} %{}: %{log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{job_name} %{build_num} %{message}\n",
                        "12345 host foo: 12345 67890 12345 67890 12345 nice_job 900 here we go again\n",
                        v(&([
                                 ("syslog_timestamp", "12345"),
                                 ("wf_host", "host"),
                                 ("log_timestamp", "12345 67890 12345 67890 12345"),
                                 ("job_name", "nice_job"),
                                 ("build_num", "900"),
                                 ("message", "here we go again")
                        ]))
                    ),

                   (
                        "%{syslog_timestamp} %{wf_host} %{} %{log_timestamp}  %{log_level}    %{main}     %{logger}%{_}%{message}%{_}",
                        "12345 host foo 12345  high    main     dummy_logger some_message  ",
                        v(&[
                                    ("syslog_timestamp", "12345"),
                                    ("wf_host", "host"),
                                    ("log_timestamp", "12345"),
                                    ("log_level", "high"),
                                    ("main", "main"),
                                    ("logger", "dummy_logger"),
                                    ("message", "some_message")
                                ])
                        ),


                        (

                        "%{syslog_timestamp} %{host} %{?kafka_tag} %{log_timestamp}: %{log_level} (%{logger}): %{full_message}",
                        "12345 foo some_tag 12345: high (dummy): welcome",
                        v(&[
                               ("syslog_timestamp", "12345"),
                                ("host", "foo"),
                                ("log_timestamp", "12345"),
                                ("log_level", "high"),
                                ("logger", "dummy"),
                                ("full_message", "welcome")
                        ])
                        ),

                        (
                            "%{syslog_timestamp} %{host} %{} %{message}",
                            "12345 foo bar here we go",
                            v(&[
                                   ("syslog_timestamp", "12345"),
                                   ("host", "foo"),
                                   ("message", "here we go")
                            ])
                            ),

                            (


                        "%{syslog_timestamp} %{host}  %{log_timestamp} %{+log_timestamp} %{message}",
                        "12345 foo  12345 67890 this works well",
                        v(&[
                               ("syslog_timestamp", "12345"),
                               ("host", "foo"),
                               ("log_timestamp", "12345 67890"),
                               ("message", "this works well")
                        ])

                        ),

        (
                        "%{syslog_timestamp} %{host}%{_}[%{log_timestamp}][%{log_level}%{_}][%{logger}%{_}] %{message}",
                        "12345 foo [12345 67890][high ][dummy ] too many brackets here",
                        v(&[
                               ("syslog_timestamp", "12345"),
                               ("host", "foo"),
                               ("log_timestamp", "12345 67890"),
                               ("log_level", "high"),
                               ("logger", "dummy"),
                               ("message", "too many brackets here")
                        ])
                        ),

        (
                        "%{syslog_timestamp} %{host}  %{} %{} %{} %{} %{syslog_program}[%{syslog_pid}]: %{message}",
                        "12345 foo  i dont care about program[12345]: some message here",
                        v(&[
                               ("syslog_timestamp", "12345"),
                               ("host", "foo"),
                               ("syslog_program", "program"),
                               ("syslog_pid", "12345"),
                               ("message", "some message here")
                            ])


                        ),
                        (

                            "%{syslog_timestamp} %{host}%{_}[%{log_timestamp}][%{log_level}%{_}][%{logger}%{_}] %{message}",
                            "12345 foo [12345][high ][dummy ] alexanderplatz",
                            v(&[
                                   ("syslog_timestamp", "12345"),
                                   ("host", "foo"),
                                   ("log_timestamp", "12345"),
                                   ("log_level", "high"),
                                   ("logger", "dummy"),
                                   ("message", "alexanderplatz")
                            ])

        ),

        (
                        "%{} %{} %{} %{source} %{}:%{message}",
                        "foo bar baz light quox:this was fun",
                        v(&[
                               ("source", "light"),
                               ("message", "this was fun")
                        ])
                        ),

                        (

                            "%{syslog_timestamp} %{wf_host}%{_}%{}: %{syslog_message}",
                            "12345 host foo: lorem ipsum",
                            v(&[
                                   ("syslog_timestamp", "12345"),
                                   ("wf_host", "host"),
                                   ("syslog_message", "lorem ipsum")
                            ])

                            ),

                            (

                "%{syslog_timestamp} %{host}%{_}%{}: %{syslog_message}",
                "12345 ghost foo: this is the last one",
                v(&[
                       ("syslog_timestamp", "12345"),
                       ("host", "ghost"),
                       ("syslog_message", "this is the last one")
                ])

                ),

                                (
                                    "this is a %{?what} named %{name}",
                                    "this is a test named cake",
                                    v(&[("name", "cake")])
                                    )

                ];

        for (pattern, input, expected) in patterns {
            assert_eq!(run(pattern, input), expected)
        }
    }

    #[test]
    fn test_patterns() {
        let _patterns_orig = vec![
        //logstash.git.transform.conf.erb
        "%{syslog_timestamp} %{wf_host} %{syslog_program}: %{syslog_message->}",
        "%{syslog_timestamp} %{wf_host} %{syslog_program}: %{syslog_message}",
        "%{}, [%{log_timestamp} #%{pid}]  %{log_level} -- %{}: %{message}",
        "%{}>%{+syslog_timestamp/1} %{+syslog_timestamp/2} %{+syslog_timestamp/3} %{syslog_hostname} %{syslog_program}: %{full_message}",
        // logstash.sox.source.conf.erb
        "%{syslog_timestamp} %{wf_host} %{}: %{log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{job_name} %{build_num} %{message} completed: %{completed}\n",
        "%{syslog_timestamp} %{wf_host} %{}: %{log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{job_name} %{build_num} %{message}\n",
        // logstash.presto.source.conf.erb
        "%{syslog_timestamp} %{wf_host} %{} %{log_timestamp}	%{log_level}	%{main}	%{logger->}	%{message->}",
        // logstash.aerospike.source.conf.erb
        "%{syslog_timestamp} %{host} %{?kafka_tag} %{log_timestamp}: %{log_level} (%{logger}): %{full_message}",
        // logstash.siem.sink.conf.erb
        "%{syslog_timestamp} %{host} %{} %{message}",
        // logstash.puppet.transform.conf.erb
        "%{syslog_timestamp} %{host} %{syslog_program}: %{syslog_message->}",
        // logstash.edw.conf.erb
        "%{syslog_timestamp} %{host}  %{log_timestamp} %{+log_timestamp} %{message}",
        // logstash.eslog.conf.erb
        "%{syslog_timestamp} %{host->} [%{log_timestamp}][%{log_level->}][%{logger->}] %{message}",
        "%{syslog_timestamp} %{host}  %{} %{} %{} %{} %{syslog_program}[%{syslog_pid}]: %{message}",
        "%{log_level} %{log_timestamp}: %{logger}: %{message}",
        "%{syslog_timestamp} %{host->} [%{log_timestamp}][%{log_level->}][%{logger->}] %{message}",
        "%{} %{} %{} %{source} %{}:%{message}",
        // logstash.misc.conf.erb
        "%{syslog_timestamp} %{wf_host->} %{}: %{syslog_message}",
        "%{syslog_timestamp} %{host->} %{}: %{syslog_message}",
    ];
        // translation:
        // remove /*
        // replace '->}' with '}%{_}'
        // remove ' ' after %{_}
        let patterns = vec![
        //logstash.git.transform.conf.erb
        "%{syslog_timestamp} %{wf_host} %{syslog_program}: %{syslog_message}%{_}",
        "%{syslog_timestamp} %{wf_host} %{syslog_program}: %{syslog_message}",
        "%{}, [%{log_timestamp} #%{pid}]  %{log_level} -- %{}: %{message}",
        "%{}>%{+syslog_timestamp} %{+syslog_timestamp} %{+syslog_timestamp} %{syslog_hostname} %{syslog_program}: %{full_message}",
        // logstash.sox.source.conf.erb
        "%{syslog_timestamp} %{wf_host} %{}: %{log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{job_name} %{build_num} %{message} completed: %{completed}\n",
        "%{syslog_timestamp} %{wf_host} %{}: %{log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{+log_timestamp} %{job_name} %{build_num} %{message}\n",
        // logstash.presto.source.conf.erb
        "%{syslog_timestamp} %{wf_host} %{} %{log_timestamp}	%{log_level}	%{main}	%{logger}%{_}%{message}%{_}",
        // logstash.aerospike.source.conf.erb
        "%{syslog_timestamp} %{host} %{?kafka_tag} %{log_timestamp}: %{log_level} (%{logger}): %{full_message}",
        // logstash.siem.sink.conf.erb
        "%{syslog_timestamp} %{host} %{} %{message}",
        // logstash.puppet.transform.conf.erb
        "%{syslog_timestamp} %{host} %{syslog_program}: %{syslog_message}%{_}",
        // logstash.edw.conf.erb
        "%{syslog_timestamp} %{host}  %{log_timestamp} %{+log_timestamp} %{message}",
        // logstash.eslog.conf.erb
        "%{syslog_timestamp} %{host}%{_}[%{log_timestamp}][%{log_level}%{_}][%{logger}%{_}] %{message}",
        "%{syslog_timestamp} %{host}  %{} %{} %{} %{} %{syslog_program}[%{syslog_pid}]: %{message}",
        "%{log_level} %{log_timestamp}: %{logger}: %{message}",
        "%{syslog_timestamp} %{host}%{_}[%{log_timestamp}][%{log_level}%{_}][%{logger}%{_}] %{message}",
        "%{} %{} %{} %{source} %{}:%{message}",
        // logstash.misc.conf.erb
        "%{syslog_timestamp} %{wf_host}%{_}%{}: %{syslog_message}",
        "%{syslog_timestamp} %{host}%{_}%{}: %{syslog_message}",
    ];
        for p in patterns {
            assert!(Pattern::compile(p).is_ok())
        }
    }
}
