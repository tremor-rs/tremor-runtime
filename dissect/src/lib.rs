// Copyright 2018-2019, Wayfair GmbH
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
//! use dissect::{dissect, Value};
//!
//! let filter = "%{a} %{b}";
//! let input = "John Doe";
//!
//! let output = dissect(filter, input).expect("");
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("a".to_owned(), Value::String("John".to_owned()));
//! expected.insert("b".to_owned(), Value::String("Doe".to_owned()));
//!
//! assert_eq!(output, expected);
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
//! use dissect::{dissect, Value};
//!
//! let output = dissect("%{name}, %{age}", "John Doe, 22").expect("");
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".to_string(), Value::String("John Doe".to_owned()));
//! expected.insert("age".to_string(), Value::String("22".to_owned()));
//!
//! assert_eq!(output, expected);
//! ```
//!
//! 2) Append (+)
//!
//! The append operator will append the value to another value creating an array.
//!
//! ```rust
//! use dissect::{dissect, Value};
//! let output = dissect("%{+name} %{+name}, %{age}", "John Doe, 22").expect("");
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".to_owned(), Value::String("John Doe".to_owned()));
//! expected.insert("age".to_owned(), Value::String("22".to_owned()));
//!
//! assert_eq!(output, expected);
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
//! use dissect::{dissect, Value};
//! let output = dissect("%{?name}, %{&name}", "John Doe, 22").expect("");
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("John Doe".to_owned(), Value::String("22".to_owned()));
//! assert_eq!(output, expected);
//!
//! ```
//!
//! 4) Empty fields
//!
//! Fields  will return an empty value if no data is present in the input.
//!
//! ```rust
//! use dissect::{dissect, Value};
//!
//! let output = dissect("%{name}, %{age}", ", 22").expect("");
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".to_owned(), Value::String("".to_owned()));
//! expected.insert("age".to_owned(), Value::String("22".to_owned()));
//! assert_eq!(output, expected);
//! ```
//!
//! 5) Skipped fields (?)
//!
//! The operator will prevent the value from being stored in the output, effectively skipping it.
//!
//! ```rust
//! use dissect::{dissect, Value};
//! let output = dissect("%{?first_name} %{last_name}, %{age}", "John Doe, 22").expect("");
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("last_name".to_owned(), Value::String("Doe".to_owned()));
//! expected.insert("age".to_owned(), Value::String("22".to_owned()));
//! assert_eq!(output, expected);
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
//! use dissect::{dissect, Value};
//! let output = dissect("%{name}, %{age:int}", "John Doe, 22").expect("");
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".to_owned(), Value::String("John Doe".to_owned()));
//! expected.insert("age".to_owned(), Value::Integer(22));
//! assert_eq!(output, expected);
//! ```
//!
//! 7) Padding (_)
//!
//! The operator will remove padding when storing the field in the output. You can specify the
//! skipped character as a parameter to `_`. It will use ` ` by default.
//!
//! ```rust
//! use dissect::{dissect, Value};
//!
//! let output = dissect("%{name}, %{_}%{age}", "John Doe,            22").expect("");
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".to_owned(), Value::String("John Doe".to_owned()));
//! expected.insert("age".to_owned(), Value::String("22".to_string()));
//! assert_eq!(output, expected);
//! ```
//!
//! ```rust
//! use dissect::{dissect, Value};
//! let output = dissect("%{name}, %{_(-)}%{age}", "John Doe, ------------22").expect("");
//! let mut expected = halfbrown::HashMap::new();
//! expected.insert("name".to_owned(), Value::String("John Doe".to_owned()));
//! expected.insert("age".to_owned(), Value::String("22".to_owned()));
//! assert_eq!(output, expected);
//! ```
//!
use halfbrown::HashMap;
use std::error::Error as ErrorTrait;
use std::fmt::{Display, Formatter, Result as FmtResult};
pub type Result<T> = std::result::Result<T, DissectError>;
pub type Pattern = Vec<Token>;

// dissect parses the pattern and returns the token extracted from the input string. It will return
// an error if either the parsing or the extracting fails, and will return an error if it it doesn't
// match the token.

pub fn dissect(pattern: &str, input: &str) -> Result<HashMap<String, Value>> {
    let tokens = lex(pattern)?;
    extract(tokens, input)
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Field(Field),
    Delimiter(String),
    Padding(String),
}

#[derive(Clone, Debug)]
pub struct Field {
    value: String,
    category: Option<FieldCategory>,
}

impl Field {
    pub fn from(value: &str, category: FieldCategory) -> Field {
        Field {
            value: value.to_owned(),
            category: Some(category),
        }
    }
}

impl PartialEq for Field {
    fn eq(&self, other: &Field) -> bool {
        self.value == other.value
    }
}

impl From<String> for Field {
    fn from(value: String) -> Field {
        Field {
            value,
            category: None,
        }
    }
}

impl From<&str> for Field {
    fn from(value: &str) -> Field {
        Field {
            value: value.to_string(),
            category: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldCategory {
    Skipped,
    Append(String),
    Typed(SupportedType),
    Named,
    NamedTyped(SupportedType),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(String),
    Integer(i32),
    Float(f32),
}

#[derive(Clone, Debug, PartialEq)]
pub enum SupportedType {
    String,
    Integer,
    Float,
}

impl Display for SupportedType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            SupportedType::String => write!(f, "string"),
            SupportedType::Integer => write!(f, "int"),
            SupportedType::Float => write!(f, "float"),
        }
    }
}

pub fn lex(pattern: &str) -> Result<Vec<Token>> {
    if pattern.is_empty() {
        Ok(vec![])
    } else {
        let tokens: Vec<usize> = pattern.match_indices("%{").map(|(k, _)| k).collect();
        if tokens.is_empty() {
            Ok(vec![(Token::Delimiter(pattern.to_string()))])
        } else {
            let mut matched_tokens = vec![];
            let iter = tokens.iter();
            let mut iter = iter.peekable();
            let len = pattern.len();

            // string starts with delimiters so we skip the tokens till we find a token.
            if tokens[0] != 0 {
                matched_tokens.push(Token::Delimiter(pattern[..tokens[0]].to_string()));
            }
            while let Some(index) = iter.next() {
                if let Some(offset) = pattern[index + 2..].find(|ch| ch == '}') {
                    let token_text = pattern[*index + 2..*index + 2 + offset].to_string();
                    match (&token_text.get(0..=0), &token_text.get(1..)) {
                        (Some("_"), Some("")) => {
                            matched_tokens.push(Token::Padding(" ".to_string()))
                        }
                        (Some("_"), Some(bracket_pad)) => {
                            if bracket_pad.starts_with('(') {
                                if let Some(pad_end) = bracket_pad.rfind(')') {
                                    matched_tokens
                                        .push(Token::Padding(bracket_pad[1..pad_end].to_string()));
                                } else {
                                    return Err(DissectError::ParseError(
                                        ParseError::InvalidPadding(bracket_pad.to_string(), *index),
                                    ));
                                }
                            } else {
                                return Err(DissectError::ParseError(ParseError::InvalidPadding(
                                    bracket_pad.to_string(),
                                    *index,
                                )));
                            }
                        }
                        (Some("?"), Some("")) => {
                            return Err(DissectError::ParseError(ParseError::InvalidToken(
                                "?".to_string(),
                                *index,
                            )));
                        }
                        (Some("?"), Some(field)) => {
                            matched_tokens
                                .push(Token::Field(Field::from(field, FieldCategory::Skipped)));
                        }
                        (Some("+"), Some(field)) => {
                            let x = matched_tokens.iter().find(|x| {
                                if let Token::Field(f) = x {
                                    f.value == *field
                                } else {
                                    false
                                }
                            });

                            match x {
                                Some(Token::Field(ref x)) => match x.category {
                                    Some(FieldCategory::Typed(_))
                                    | Some(FieldCategory::NamedTyped(_)) => {
                                        let cix =
                                            x.value.find(':').unwrap_or_else(|| x.value.len());

                                        return Err(DissectError::ParseError(
                                            ParseError::AppendDoesNotSupportTypes(
                                                x.value[0..cix].to_owned(),
                                                *index,
                                            ),
                                        ));
                                    }
                                    _ => {
                                        if field.contains(':') {
                                            return Err(DissectError::ParseError(
                                                ParseError::AppendDoesNotSupportTypes(
                                                    x.value.to_owned(),
                                                    *index,
                                                ),
                                            ));
                                        } else {
                                            matched_tokens.push(Token::Field(Field::from(
                                                field,
                                                FieldCategory::Append(field.to_owned().to_owned()),
                                            )));
                                        }
                                    }
                                },
                                _ => {
                                    if field.contains(':') {
                                        let cix = field.find(':').unwrap_or_else(|| field.len());

                                        return Err(DissectError::ParseError(
                                            ParseError::AppendDoesNotSupportTypes(
                                                field[0..cix].to_owned().to_owned(),
                                                *index,
                                            ),
                                        ));
                                    } else {
                                        matched_tokens.push(Token::Field(Field::from(
                                            field,
                                            FieldCategory::Append(field.to_owned().to_owned()),
                                        )));
                                    }
                                }
                            }
                        }

                        (Some("&"), Some(field)) => {
                            if field.contains(':') {
                                let parts = field.split(':').collect::<Vec<&str>>();
                                let field = parts[0];
                                if parts.len() == 2 {
                                    let datatype = match parts[1] {
                                        "int" => Ok(SupportedType::Integer),
                                        "float" => Ok(SupportedType::Float),
                                        "string" => Ok(SupportedType::String),
                                        _ => Err(DissectError::ParseError(
                                            ParseError::TypeNotSupported(parts[1].to_string()),
                                        )),
                                    }?;

                                    matched_tokens.push(Token::Field(Field::from(
                                        field,
                                        FieldCategory::NamedTyped(datatype),
                                    )));
                                } else {
                                    return Err(DissectError::ParseError(
                                        ParseError::InvalidToken(field.to_owned(), *index),
                                    ));
                                }
                            } else {
                                matched_tokens
                                    .push(Token::Field(Field::from(field, FieldCategory::Named)));
                            }
                        }

                        (Some(f), token) => {
                            let token = f.to_owned().to_owned() + token.unwrap_or("");

                            let parts = token.split(':').collect::<Vec<&str>>();

                            match parts.len() {
                                1 => {
                                    if let Some(Token::Field(_)) = matched_tokens.last() {
                                        return Err(DissectError::ParseError(
                                            ParseError::NoDelimiter(*index),
                                        ));
                                    } else {
                                        matched_tokens.push(Token::Field(token_text.into()));
                                    }
                                }

                                2 => {
                                    let field = parts[0];
                                    let datatype = match parts[1].trim() {
                                        "int" => Ok(SupportedType::Integer),
                                        "float" => Ok(SupportedType::Float),
                                        "string" => Ok(SupportedType::String),
                                        _ => Err(DissectError::ParseError(
                                            ParseError::TypeNotSupported(parts[1].to_string()),
                                        )),
                                    }?;

                                    matched_tokens.push(Token::Field(Field::from(
                                        field,
                                        FieldCategory::Typed(datatype),
                                    )));
                                }
                                _ => {
                                    return Err(DissectError::ParseError(
                                        ParseError::InvalidToken(token, *index),
                                    ));
                                }
                            }
                        }
                        (None, _) => {
                            matched_tokens
                                .push(Token::Field(Field::from("", FieldCategory::Skipped)));
                        }
                    }

                    match iter.peek() {
                        Some(delim_index) if **delim_index < offset => {
                            return Err(DissectError::ParseError(
                                ParseError::MissingClosingBracket(*index),
                            ));
                        }
                        Some(delim_index) => {
                            let delimiter = pattern[index + offset + 3..**delim_index].to_string();
                            if !delimiter.is_empty() {
                                matched_tokens.push(Token::Delimiter(delimiter));
                            }
                        }
                        _ => {
                            let delimiter = pattern[index + offset + 3..].to_string();
                            if !delimiter.is_empty() {
                                matched_tokens.push(Token::Delimiter(delimiter));
                            }
                        }
                    }

                    pattern[index + 2 + offset + 1..**iter.peek().unwrap_or(&&len)].to_string();
                } else {
                    return Err(DissectError::ParseError(ParseError::MissingClosingBracket(
                        *index,
                    )));
                }
            }
            Ok(matched_tokens)
        }
    }
}

pub fn extract(tokens: Vec<Token>, input: &str) -> Result<HashMap<String, Value>> {
    let iter = tokens.iter();
    let mut data = input;
    let mut iter = iter.enumerate().peekable();
    let mut output = HashMap::<String, Value>::new();
    let mut skipped_values = vec![];
    let mut delimiters = vec![];
    while let Some((idx, token)) = iter.next() {
        match token {
            Token::Field(ref pat) => {
                let (extract, d) = match iter.peek() {
                    Some((_, Token::Delimiter(delim))) => match data.find(delim.as_str()) {
                        Some(x) => {
                            delimiters.push((pat, delim));
                            (data[..x].to_string(), &data[x..])
                        }
                        None => {
                            return Err(DissectError::RuntimeError(
                                RuntimeError::NoDelimiterInInput(delim.to_owned()),
                            ));
                        }
                    },

                    Some((_, Token::Padding(_))) => {
                        let pad_iter = iter.clone();

                        let mut stop_after_first_delim = true;
                        let non_tokens: Vec<Token> = pad_iter
                            .take_while(|(_, x)| match x {
                                Token::Padding(_) => true,
                                Token::Delimiter(_) if stop_after_first_delim => {
                                    stop_after_first_delim = false;
                                    true
                                }

                                _ => false,
                            })
                            .map(|(_, x)| x.to_owned())
                            .collect();

                        let mut ex = (String::from(data), "");

                        let _ = non_tokens.iter().rev().find(|x| match x {
                            Token::Delimiter(delim) | Token::Padding(delim) => {
                                match data.find(delim) {
                                    Some(y) => {
                                        ex = (data[..y].to_owned(), &data[y..]);
                                        true
                                    }
                                    None => false,
                                }
                            }
                            _ => {
                                ex = (data.to_owned(), "");
                                false
                            }
                        });

                        non_tokens.iter().rev().for_each(|nt| {
                            if let Token::Padding(p) = nt {
                                ex.0 = ex.0.trim_end_matches(p).to_owned();
                                ex.1 = ex.1.trim_start_matches(p);
                            }
                        });

                        ex
                    }
                    None => {
                        if data == "" {
                            return Err(DissectError::RuntimeError(
                                RuntimeError::AllTokensNotExhausted(idx),
                            ));
                        } else {
                            (data.to_owned(), "")
                        }
                    }
                    // two consecutive tokens is invalid
                    _ => unreachable!(),
                };

                match pat.category {
                    Some(FieldCategory::Skipped) => {
                        skipped_values.push((pat.value.to_owned(), extract));
                    }
                    Some(FieldCategory::Typed(ref datatype)) => {
                        let value = match datatype {
                            SupportedType::Integer => {
                                extract.parse::<i32>().map(Value::Integer).map_err(|_| {
                                    DissectError::RuntimeError(
                                        RuntimeError::CannotParseValueToType(
                                            extract,
                                            SupportedType::Integer,
                                        ),
                                    )
                                })?
                            }
                            SupportedType::Float => {
                                extract.parse::<f32>().map(Value::Float).map_err(|_| {
                                    DissectError::RuntimeError(
                                        RuntimeError::CannotParseValueToType(
                                            extract,
                                            SupportedType::Float,
                                        ),
                                    )
                                })?
                            }
                            _ => Value::String(extract.to_owned()),
                        };

                        output.insert(pat.value.to_owned(), value);
                    }

                    Some(FieldCategory::Named) => {
                        match skipped_values.iter().rev().find(|(x, _)| *x == pat.value) {
                            Some((_, match_left)) => {
                                output.insert(match_left.to_string(), Value::String(extract));
                            }
                            None => {
                                return Err(DissectError::RuntimeError(
                                    RuntimeError::FieldNameNotFound(pat.value.to_string()),
                                ));
                            }
                        }
                    }

                    Some(FieldCategory::NamedTyped(ref datatype)) => {
                        match skipped_values.iter().rev().find(|(x, _)| *x == pat.value) {
                            Some((_, match_left)) => {
                                let value = match datatype {
                                    SupportedType::Integer => extract
                                        .parse::<i32>()
                                        .map(Value::Integer)
                                        .map_err(|_| {
                                            DissectError::RuntimeError(
                                                RuntimeError::CannotParseValueToType(
                                                    extract,
                                                    SupportedType::Integer,
                                                ),
                                            )
                                        })?,

                                    SupportedType::Float => {
                                        extract.parse::<f32>().map(Value::Float).map_err(|_| {
                                            DissectError::RuntimeError(
                                                RuntimeError::CannotParseValueToType(
                                                    extract,
                                                    SupportedType::Float,
                                                ),
                                            )
                                        })?
                                    }
                                    SupportedType::String => Value::String(extract.to_string()),
                                };

                                output.insert(match_left.to_owned(), value);
                            }
                            None => {
                                return Err(DissectError::RuntimeError(
                                    RuntimeError::FieldNameNotFound(pat.value.to_string()),
                                ));
                            }
                        }
                    }
                    Some(FieldCategory::Append(_)) => match output.get_mut(&pat.value) {
                        Some(entry) => match entry {
                            Value::String(s) => {
                                let en = match iter.peek() {
                                    Some((_, Token::Delimiter(_))) => {
                                        match delimiters.iter().find(|(p, _)| p.value == pat.value)
                                        {
                                            Some((_, prev_delim)) => {
                                                Value::String(s.to_owned() + prev_delim + &extract)
                                            }

                                            _ => Value::String(s.to_owned() + &extract),
                                        }
                                    }
                                    None => {
                                        if let Some((_, prev_delim)) =
                                            delimiters.iter().find(|(p, _)| p.value == pat.value)
                                        {
                                            Value::String(s.to_owned() + prev_delim + &extract)
                                        } else {
                                            Value::String(s.to_owned() + &extract)
                                        }
                                    }
                                    _ => unreachable!(),
                                };
                                output.insert(pat.value.to_owned(), en);
                            }
                            _ => unreachable!(),
                        },
                        _ => {
                            output.insert(pat.value.to_owned(), Value::String(extract.to_string()));
                        }
                    },
                    _ => {
                        output.insert(pat.value.to_owned(), Value::String(extract.to_string()));
                    }
                }
                data = d;
            }

            Token::Delimiter(d) => {
                if data.starts_with(d.as_str()) {
                    data = &data[d.len()..];
                } else {
                    return Err(DissectError::RuntimeError(
                        RuntimeError::NoDelimiterInInput(d.to_owned()),
                    ));
                }
            }

            Token::Padding(pad) => {
                data = &data.trim_start_matches(pad.as_str());
                /*
                match iter.peek() {
                    Some((_, Token::Field(_))) | SomeToken::Delimiter(d) | None => {
                        data = &data.trim_start_matches(pad.as_str());
                    }
                    x => {
                        dbg!(x);
                    }
                    };
                    */
            }
        }
    }

    if !data.is_empty() {
        let len = input.find(data).unwrap_or(0);
        Err(DissectError::RuntimeError(
            RuntimeError::InputNotCompletelyParsed(len),
        ))
    } else {
        Ok(output)
    }
}
#[derive(Debug, PartialEq)]
pub enum ParseError {
    MissingClosingBracket(usize),
    NoDelimiter(usize),
    InvalidPadding(String, usize),
    InvalidToken(String, usize),
    TypeNotSupported(String),
    AppendDoesNotSupportTypes(String, usize),
}

#[derive(Debug, PartialEq)]
pub enum RuntimeError {
    NoDelimiterInInput(String),
    FieldNameNotFound(String),
    CannotParseValueToType(String, SupportedType),
    InputNotCompletelyParsed(usize),
    AllTokensNotExhausted(usize),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            ParseError::MissingClosingBracket(index) => {
                write!(f, "The {{ at {} has no matching }}", index)
            }
            ParseError::NoDelimiter(index) => write!(f, "No delimiter between tokens at {}", index),
            ParseError::InvalidPadding(pad, index) => {
                write!(f, "Invalid padding {} at {}", pad, index)
            }
            ParseError::InvalidToken(token, index) => {
                write!(f, "Invalid token {} at {}", token, index)
            }
            ParseError::TypeNotSupported(datatype) => write!(f, "Type {} not supported", datatype),
            ParseError::AppendDoesNotSupportTypes(field, _) => {
                write!(f, "Can't use types with append for field {}", field)
            }
        }
    }
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            RuntimeError::NoDelimiterInInput(delim) => {
                write!(f, "Missing delimiter {} in the input", delim)
            }
            RuntimeError::FieldNameNotFound(field) => write!(f, "named field {} not found", field),
            RuntimeError::CannotParseValueToType(value, datatype) => {
                write!(f, "Cannot parse value {} to type {}", value, datatype)
            }
            RuntimeError::InputNotCompletelyParsed(index) => {
                write!(f, "Input not completely parsed starting at {}", index)
            }
            RuntimeError::AllTokensNotExhausted(idx) => {
                write!(f, "Tokens still remaining to be parsed from {}", idx)
            }
        }
    }
}

impl ErrorTrait for ParseError {}
impl ErrorTrait for RuntimeError {}

#[derive(Debug, PartialEq)]
pub enum DissectError {
    ParseError(ParseError),
    RuntimeError(RuntimeError),
}

impl Display for DissectError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
pub mod acceptance;
#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Token {
        Token::Field(s.into())
    }

    fn d(s: &str) -> Token {
        (Token::Delimiter(s.to_string()))
    }

    fn v(s: &[(&str, &str)]) -> HashMap<String, Value> {
        s.iter()
            .map(|(x, y)| (x.to_string(), Value::String(y.to_string())))
            .collect()
    }

    fn pad(s: &str) -> Token {
        Token::Padding(s.to_string())
    }

    #[test]
    fn parse_empty_tokens() {
        let input = "";
        let output = lex(input);

        assert_eq!(output.expect(""), vec![]);
    }

    #[test]
    fn parse_no_tokens() {
        let input = "a";
        let output = lex(input);

        assert_eq!(output.expect(""), vec![d("a")]);
    }

    #[test]
    fn parse_one_token() {
        let input = "%{name}";
        let output = lex(input);

        assert_eq!(output.expect(""), vec![p("name")]);
    }

    #[test]
    fn parse_multiple_tokens() {
        let filter = "%{a} %{b}";
        //        let input = "John Doe";

        //    let output = dissect(filter, input);
        //   assert_eq!(output, vec![("a", "John"), ("b", "Doe")]);

        let output = lex(filter).expect("");
        assert_eq!(output, vec![p("a"), d(" "), p("b")]);
    }

    #[test]
    fn parse_stray_opening_bracket_returns_an_error() {
        let filter = "%{a";
        let output = lex(filter);
        assert_eq!(
            output,
            Err(DissectError::ParseError(ParseError::MissingClosingBracket(
                0
            )))
        );
    }

    #[test]
    fn parse_stray_closing_backet_is_considered_a_literal() {
        let filter = "}";
        let output = lex(filter).expect("");
        assert_eq!(output, vec![d("}")]);
    }

    #[test]
    fn parse_literal_percentage() {
        let filter = "%{a}%";
        let output = lex(filter).expect("");
        assert_eq!(output, vec![p("a"), d("%")]);
    }

    #[test]
    fn parse_all_edgecases() {
        let testcases = [
            ("%{name}}%{age}", vec![p("name"), d("}"), p("age")]),
            ("%{name}%%{age}", vec![p("name"), d("%"), p("age")]),
            (".%{name}", vec![d("."), p("name")]),
            ("foo %{name}", vec![d("foo "), p("name")]),
            ("foo %{name} bar", vec![d("foo "), p("name"), d(" bar")]),
            ("%{name} bar", vec![p("name"), d(" bar")]),
            ("%{name}bar", vec![p("name"), d("bar")]),
            ("name%{bar}", vec![d("name"), p("bar")]),
            (
                "%{name} %{age} %{country}",
                vec![p("name"), d(" "), p("age"), d(" "), p("country")],
            ),
            (
                "%{name} %{age}-%{country}",
                vec![p("name"), d(" "), p("age"), d("-"), p("country")],
            ),
            ("this is %{test}", vec![d("this is "), p("test")]),
            ("%{test} case", vec![p("test"), d(" case")]),
            (
                "this is %{test} case",
                vec![d("this is "), p("test"), d(" case")],
            ),
            (
                "this is %{test} case named %{name}",
                vec![d("this is "), p("test"), d(" case named "), p("name")],
            ),
            (
                "this is %{test} case named %{?name}",
                vec![
                    d("this is "),
                    p("test"),
                    d(" case named "),
                    Token::Field(Field {
                        value: "name".to_string(),
                        category: Some(FieldCategory::Skipped),
                    }),
                ],
            ),
        ];

        for case in testcases.iter() {
            let output = lex(case.0).expect("");
            assert_eq!(output, case.1);
        }
    }

    #[test]
    fn parse_no_delimiter_returns_an_error() {
        let input = "%{name}%{bar}";
        let output = lex(input);
        assert_eq!(
            output,
            Err(DissectError::ParseError(ParseError::NoDelimiter(7)))
        );
    }

    #[test]
    fn extract_simple_case() {
        let pattern = vec![p("name")];
        let output = extract(pattern, "John Doe").expect("");
        let mut expected = HashMap::new();
        expected.insert("name".to_string(), Value::String("John Doe".to_string()));
        assert_eq!(output, expected);
    }

    #[test]
    fn dissect_all_edgecases() {
        let testcases = vec![
            (
                "%{name}}%{age}",
                "John}22",
                Ok(v(&[("name", "John"), ("age", "22")])),
            ),
            (
                "%{name}%%{age}",
                "John%22",
                Ok(v(&[("name", "John"), ("age", "22")])),
            ),
            (
                "%{name}%%{age}",
                "John}22",
                Err(DissectError::RuntimeError(
                    RuntimeError::NoDelimiterInInput("%".to_owned()),
                )),
            ),
            (".%{name}", ".John", Ok(v(&[("name", "John")]))),
            (
                ".%{name}",
                "John",
                Err(DissectError::RuntimeError(
                    RuntimeError::NoDelimiterInInput(".".to_owned()),
                )),
            ),
            ("foo %{name}", "foo John", Ok(v(&[("name", "John")]))),
            (
                "foo %{name} bar",
                "foo John bar",
                Ok(v(&[("name", "John")])),
            ),
            ("%{name} bar", "John bar", Ok(v(&[("name", "John")]))),
            ("%{name}bar", "Johnbar", Ok(v(&[("name", "John")]))),
            ("name%{bar}", "nameJohn", Ok(v(&[("bar", "John")]))),
            (
                "%{name} %{age} %{country}",
                "John 22 Germany",
                Ok(v(&[
                    ("name", "John"),
                    ("age", "22"),
                    ("country", "Germany"),
                ])),
            ),
            (
                "%{name} %{age}-%{country}",
                "John 22-Germany",
                Ok(v(&[
                    ("name", "John"),
                    ("age", "22"),
                    ("country", "Germany"),
                ])),
            ),
            (
                "this is a %{name} case",
                "this is a John case",
                Ok(v(&([("name", "John")]))),
            ),
            (
                "this is a %{what} case named %{name}",
                "this is a test case named John",
                Ok(v(&([("what", "test"), ("name", "John")]))),
            ),
            (
                "this is a %{what}%{_}case named %{name}",
                "this is a test  case named John",
                Ok(v(&[("what", "test"), ("name", "John")])),
            ),
            (
                "this is a %{arr} %{+arr}",
                "this is a test case",
                Ok(v(&[("arr", "test case")])),
            ),
            (
                "%{name}%{_}%{_(|)}/%{age}",
                "John/22",
                Ok(v(&[("name", "John"), ("age", "22")])),
            ),
            (
                "%{name}%{_}%{_(|)}/%{age}",
                "John|/22",
                Ok(v(&[("name", "John"), ("age", "22")])),
            ),
            (
                "%{name}%{_}%{_(|)}/%{age}",
                "John /22",
                Ok(v(&[("name", "John"), ("age", "22")])),
            ),
            (
                "%{name}%{_}%{_(|)}/ %{age}",
                "John|/ 22",
                Ok(v(&[("name", "John"), ("age", "22")])),
            ),
            (
                "%{name}%{_}%{_(|)}%{age}",
                "John||22",
                Ok(v(&[("name", "John"), ("age", "22")])),
            ),
            (
                "%{name}%{_}%{_(|)}%{age}",
                "John 22",
                Ok(v(&[("name", "John"), ("age", "22")])),
            ),
            ("%{name} cake", "John cake", Ok(v(&[("name", "John")]))),
            (
                "%{name} cake",
                "John",
                Err(DissectError::RuntimeError(
                    RuntimeError::NoDelimiterInInput(" cake".to_owned()),
                )),
            ),
            (
                "%{name}%{_}%{_(|)}%{age}",
                "John22",
                Err(DissectError::RuntimeError(
                    RuntimeError::AllTokensNotExhausted(3),
                )),
            ),
            (
                "%{a}%{_}%{b}",
                "this    works",
                Ok(v(&[("a", "this"), ("b", "works")])),
            ),
            ("%{a}%{_}", "this   ", Ok(v(&[("a", "this")]))),
            ("%{a}%{_}", "this", Ok(v(&[("a", "this")]))),
        ];

        testcases
            .into_iter()
            .for_each(|(pattern, input, expected)| {
                let output = dissect(pattern, input);
                assert_eq!(output, expected);
            });
    }

    #[test]
    fn two_pads() {
        let pattern = "%{_}this%{_}%{_(-)}works";
        let input = "this     -----works";
        let output = dissect(pattern, input).expect("failed to run pettern");
        let m = HashMap::new();
        assert_eq!(output, m)
    }

    #[test]
    fn forbidden_testcase() {
        let input = "%{a %{b}";
        let output = lex(input);
        assert_eq!(
            output,
            Err(DissectError::ParseError(ParseError::MissingClosingBracket(
                0
            )))
        );
    }

    #[test]
    fn forbidden_idx_testcase() {
        let input = "this is %{test case";
        let output = lex(input);
        assert_eq!(
            output,
            Err(DissectError::ParseError(ParseError::MissingClosingBracket(
                8
            )))
        );
    }

    #[test]
    fn parse_pattern_with_simple_case_of_padding() {
        let input = "%{name}%{_}%{age}";
        let output = lex(input).expect("");
        assert_eq!(output, vec![p("name"), pad(" "), p("age")]);
    }

    #[test]
    fn parse_pattern_with_right_padding() {
        let input = "%{name}%{_}";
        let output = lex(input).expect("");
        assert_eq!(output, vec![p("name"), pad(" ")]);
    }

    #[test]
    fn parse_pattern_with_left_padding() {
        let input = "%{_}%{name}";
        let output = lex(input).expect("");
        assert_eq!(output, vec![pad(" "), p("name")]);
    }

    #[test]
    fn parse_disect_with_padding() {
        let input = "%{name}%{_}%{age}";
        let output = dissect(input, "John   22").expect("");
        assert_eq!(output, v(&[("name", "John"), ("age", "22")]));
    }

    #[test]
    fn dissect_string_with_delimiter_at_the_end_returns_err() {
        let pattern = "%{syslog_timestamp} %{wf_host} %{syslog_program}: %{syslog_message}%{_}";
        let input = "2019-04-26 tremor-host tremor: dissect is working fine";
        assert_eq!(
            dissect(pattern, input),
            Err(DissectError::RuntimeError(
                RuntimeError::InputNotCompletelyParsed(39)
            ))
        );
    }

    #[test]
    fn dissect_string_with_padding_as_prefix() {
        let pattern = "%{_}%{name}";
        let input = "    John";
        assert_eq!(dissect(pattern, input).expect(""), v(&[("name", "John")]));
    }

    #[test]
    fn dissect_with_padding_as_a_pattern_but_no_padding_in_the_text() {
        let pattern = "%{name}%{_}";
        let input = "John";
        assert_eq!(dissect(pattern, input).expect(""), v(&[("name", "John")]));
    }

    #[test]
    fn dissect_with_optional_padding_in_the_middle() {
        let pattern = "%{name}%{_}|%{age}";
        let input = "John|22";
        assert_eq!(
            dissect(pattern, input).expect(""),
            v(&[("name", "John"), ("age", "22")])
        );
    }

    #[test]
    fn dissect_custom_padding() {
        let pattern = "%{name}%{_(|)}";
        let input = "John||||";
        assert_eq!(dissect(pattern, input).expect(""), v(&[("name", "John")]));
    }

    #[test]
    fn dissect_custom_optional_padding() {
        let pattern = "%{name}%{_(|)}";
        let input = "John";
        assert_eq!(dissect(pattern, input).expect(""), v(&[("name", "John")]));
    }

    #[test]
    fn dissect_padding_in_the_middle() {
        let pattern = "%{name}%{_(|)}%{age}";
        let input = "John||22";
        assert_eq!(
            dissect(pattern, input).expect(""),
            v(&[("name", "John"), ("age", "22")])
        );
    }

    #[test]
    fn dissect_multiple_padding() {
        let pattern = "%{name}%{_}%{_(|)}";
        let input = "John  ||";
        assert_eq!(dissect(pattern, input).expect(""), v(&[("name", "John")]));
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle() {
        let pattern = "%{name}%{_}%{_(|)}%{age}";
        let input = "John  ||22";
        assert_eq!(
            dissect(pattern, input).expect(""),
            v(&[("name", "John"), ("age", "22")])
        );
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle_with_delim_err() {
        let pattern = "%{name}%{_}%{_(|)}/ %{age}";
        let input = "John |/ 22";
        let output = dissect(pattern, input).expect("");
        assert_eq!(output, v(&([("name", "John"), ("age", "22")])));
    }

    #[test]
    fn dissect_skipped_fields() {
        let pattern = "%{?first_name} %{last_name}";
        let input = "John Doe";
        assert_eq!(
            dissect(pattern, input).expect("skipped fields doesn't work"),
            v(&[("last_name", "Doe")])
        );
    }

    #[test]
    fn question_mark_with_nothing_doesnt_parse() {
        let pattern = "%{?} %{something}";
        let output = lex(pattern);
        assert_eq!(
            output,
            Err(DissectError::ParseError(ParseError::InvalidToken(
                "?".to_owned(),
                0
            )))
        );
    }

    #[test]
    fn dissect_skipped_field_alone() {
        let pattern = "%{?foo}";
        let input = "John";
        let output = dissect(pattern, input).expect("plain skipped field doesnt work");
        assert_eq!(output, HashMap::new());
    }

    #[test]
    fn dissect_multiple_skipped_fields() {
        let pattern = "%{?first_name} %{?middle_name} %{last_name}";
        let input = "John Michael Doe";
        let output = dissect(pattern, input).expect("multiple skipped fields doesn't work");
        assert_eq!(output, v(&[("last_name", "Doe")]));
    }

    #[test]
    fn dissect_skipped_field_at_eol() {
        let pattern = "%{first_name} %{?last_name}";
        let input = "John Doe";
        let output = dissect(pattern, input).expect("dissect doesn't skip fields at end of line");
        assert_eq!(output, v(&[("first_name", "John")]));
    }

    #[test]
    fn dissect_with_integer_type() {
        let pattern = "%{name} %{age:int}";
        let input = "John 22";
        let output = dissect(pattern, input).expect("dissect doesn't do types");
        let mut expected = HashMap::new();
        expected.insert("name".to_owned(), Value::String("John".to_owned()));
        expected.insert("age".to_owned(), Value::Integer(22));
        assert_eq!(output, expected);
    }

    #[test]
    fn dissect_with_float_type() {
        let pattern = "%{name} %{tax:float}";
        let input = "John 414.203";
        let output = dissect(pattern, input).expect("dissect doesn't float");
        let mut expected = HashMap::new();
        expected.insert("name".to_owned(), Value::String("John".to_owned()));
        expected.insert("tax".to_owned(), Value::Float(414.203));

        assert_eq!(output, expected);
    }

    #[test]
    fn dissect_with_string_is_a_noop() {
        let pattern = "%{name:string}";
        let input = "John";
        let output = dissect(pattern, input).expect(
            "dissect doesn't work with string as a type even though it is a string already",
        );
        let mut expected = HashMap::new();
        expected.insert("name".to_owned(), Value::String("John".to_owned()));

        assert_eq!(output, expected);
    }

    #[test]
    fn dissect_with_wrong_type_errors_out() {
        let pattern = "%{name} %{age:wakanda}";
        let input = "John 33";
        let output = dissect(pattern, input);
        assert_eq!(
            output,
            Err(DissectError::ParseError(ParseError::TypeNotSupported(
                "wakanda".to_string()
            )))
        );
    }

    #[test]
    fn lex_multiple_colons_is_invalid() {
        let input = "%{foo:int:float}";
        let output = lex(input);
        assert_eq!(
            output,
            Err(DissectError::ParseError(ParseError::InvalidToken(
                "foo:int:float".to_owned(),
                0
            )))
        );
    }

    #[test]
    fn dissect_with_no_field_name() {
        let pattern = "%{} %{name}";
        let input = "Foo John";
        let output = dissect(pattern, input).expect("dissect doesn't like empty fields");
        assert_eq!(output, v(&[("name", "John")]));
    }

    #[test]
    fn dissect_append_field() {
        let pattern = "%{+name} %{+name} %{age}";
        let input = "John Doe 22";
        let output = dissect(pattern, input).expect("dissect doesn't like append");
        assert_eq!(output, v(&[("name", "John Doe"), ("age", "22")]));
    }

    #[test]
    fn lex_simple_append() {
        let input = "%{+name} %{+name}";
        let output = lex(input).expect("doesn't lex appends");

        assert_eq!(
            output,
            vec![
                Token::Field(Field {
                    value: "name".to_owned(),
                    category: Some(FieldCategory::Append("name".to_string()))
                }),
                Token::Delimiter(" ".to_string()),
                Token::Field(Field {
                    value: "name".to_owned(),
                    category: Some(FieldCategory::Append("name".to_string()))
                })
            ]
        );
    }

    #[test]
    fn dissect_simple_append() {
        let pattern = "%{+name} %{+name}";
        let input = "John Doe";
        let output = dissect(pattern, input).expect("dissect doesn't append");
        assert_eq!(output, v(&[("name", "John Doe")]));
    }

    #[test]
    fn dissect_out_of_order_append() {
        let pattern = "%{+name} %{age} %{+name}";
        let input = "John 22 Doe";
        let output = dissect(pattern, input).expect("out of order");
        assert_eq!(output, v(&[("name", "John Doe"), ("age", "22")]));
    }

    #[test]
    fn dissect_multiple_append() {
        let pattern = "%{+name} %{+country}|%{+name} %{+country}";
        let input = "John United|Doe States";
        let output = dissect(pattern, input).expect("doesn't work with 2 appends");
        assert_eq!(
            output,
            v(&[("name", "John Doe"), ("country", "United|States")])
        )
    }

    #[test]
    fn lex_fails_for_append_and_types() {
        let pattern = "%{+age:int}";
        let output = lex(pattern);
        assert_eq!(
            output,
            Err(DissectError::ParseError(
                ParseError::AppendDoesNotSupportTypes("age".to_owned(), 0)
            ))
        );
    }

    #[test]
    fn lex_fails_for_append_with_type_on_append() {
        let pattern = "%{age} %{+age:int}";
        let output = dissect(pattern, "22 23");
        assert_eq!(
            output,
            Err(DissectError::ParseError(
                ParseError::AppendDoesNotSupportTypes("age".to_owned(), 7)
            ))
        )
    }

    #[test]
    fn lex_fails_for_append_with_type_on_non_append() {
        let pattern = "%{age:int} %{+age}";

        let output = dissect(pattern, "22 23");
        assert_eq!(
            output,
            Err(DissectError::ParseError(
                ParseError::AppendDoesNotSupportTypes("age".to_owned(), 11)
            ))
        );
    }

    #[test]
    fn dissect_named_keys() {
        let pattern = "%{?name}, %{&name}";
        let input = "John Doe, 22";
        let output = dissect(pattern, input).expect("dissect doesn't work with named values");
        assert_eq!(output, v(&[("John Doe", "22")]));
    }

    #[test]
    fn dissect_named_keys_complex_case() {
        let pattern = "%{?name}, %{age} %{&name}";
        let input = "John, 22 Doe";
        let output = dissect(pattern, input).expect("dissect needs named in order");
        assert_eq!(output, v(&([("age", "22"), ("John", "Doe")])));
    }

    #[test]
    fn dissect_named_keys_with_values() {
        let pattern = "%{?name}, %{&name:int}";
        let input = "John Doe, 22";
        let output = dissect(pattern, input).expect("dissect doesn't do named types");
        let mut expected = HashMap::new();
        expected.insert("John Doe".to_owned(), Value::Integer(22));

        assert_eq!(output, expected);
    }

    #[test]
    fn lex_weblog() {
        let pattern = r#"%{syslog_timestamp} %{syslog_hostname} %{?syslog_prog}: %{syslog_program_aux}[%{syslog_pid:int}] %{request_unix_time} %{request_timestamp} %{request_elapsed_time} %{server_addr}:%{server_port:int} %{remote_addr}:%{remote_port:int} "%{response_content_type}" %{response_content_length} %{request_status} %{bytes_sent} %{request_length} "%{url_scheme}" "%{http_host}" "%{request_method} %{request_url} %{request_protocol}" "%{http_referer}" "%{http_user_agent}" "%{http_x_forwarded_for}" "%{http_ttrue_client_ip}" "%{remote_user}" "%{is_bot}" "%{admin_user}" "%{http_via}" "%{response_location}" "%{set_cookie}" "%{http_cookie}" "%{moawsl_info}" "%{php_message}" "%{akamai_edgescape}" "%{uid_info}" "%{geoip_country}" "%{geoip_region}" "%{geoip_city}" "%{geoip_postal}" "%{geoip_dma}" "%{server_id}" "%{txid}" "%{hpcnt}" "%{client_accept}" "%{client_accept_charset}" "%{client_accept_encoding}" "%{client_accept_language}" "%{client_accept_datetime}" "%{client_pragma}" "%{client_transfer_encoding}" "%{client_attdeviceid}" "%{client_wap_profile}" %{weblog_end}"#;

        assert!(lex(pattern).is_ok());
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

        patterns.iter().for_each(|(pattern, input, expected)| {
            let output = dissect(pattern, input).expect("");
            assert_eq!(output, *expected);
        });
    }

    #[test]
    pub fn dissect_empty() {
        let output = dissect("", "").expect("");
        assert_eq!(output, HashMap::new());
    }

    #[test]
    pub fn multi_pad() {
        let pattern = "|%{n}%{_}|";
        let input = "|foo |";
        let output = dissect(pattern, input).expect("");
        let second_op = dissect(pattern, "|foo|").expect("");
        assert_eq!(output, v(&[("n", "foo")]));
        assert_eq!(second_op, v(&[("n", "foo")]));
    }

    #[test]
    pub fn extract_with_padding_specific() {
        let pattern = "this is a %{what}%{_( case)} named %{name}";
        let input = "this is a test case named cake";
        let output = dissect(pattern, input).expect("");
        assert_eq!(output, v(&[("what", "test"), ("name", "cake")]));
    }

    #[test]
    pub fn do_kv() {
        let pattern = "this is a %{?name} case named %{&name}";
        let input = "this is a test case named cake";
        let output = dissect(pattern, input).expect("");
        assert_eq!(output, v(&[("test", "cake")]));
    }

    #[test]
    pub fn do_repeat_kv() {
        let pattern = "%{?count}: %{&count:int}, %{?count}: %{&count:int}";
        let input = "tremor: 1, logstash: 0";
        let output = dissect(pattern, input).expect("");
        let mut expected = HashMap::new();
        expected.insert("tremor".to_owned(), Value::Integer(1));
        expected.insert("logstash".to_owned(), Value::Integer(0));
        assert_eq!(output, expected);
    }

    #[test]
    pub fn parsing_wrong_value_returns_error() {
        let pattern = "%{foo:int}";
        let input = "abcdef";
        let output = dissect(pattern, input);
        assert_eq!(
            output,
            Err(DissectError::RuntimeError(
                RuntimeError::CannotParseValueToType("abcdef".to_owned(), SupportedType::Integer),
            )),
        );
    }

    #[test]
    pub fn consecutive_padding_first_one_optional() {
        let pattern = "%{name}%{_}%{_(|)}%{age}";
        let input = "John|22";
        let output = dissect(pattern, input).expect("");
        let mut expected = HashMap::new();
        expected.insert("name".to_owned(), Value::String("John".to_string()));
        expected.insert("age".to_owned(), Value::String("22".to_string()));
        assert_eq!(output, expected);
    }

    #[test]
    fn empty() {
        assert_eq!(lex("").expect("failed to compile pattern"), vec![]);
    }

    #[test]
    fn type_float() {
        let pattern = "%{f:float}";
        let p = lex(pattern).expect("failed to compile pattern");
        assert_eq!(
            p,
            vec![Token::Field(Field {
                value: "f".to_owned(),
                category: Some(FieldCategory::Typed(SupportedType::Float))
            })]
        );

        let mut m = HashMap::new();
        m.insert("f".to_string(), Value::Float(1.0));

        assert_eq!(dissect(pattern, "1.0").expect(""), m.clone());
        assert_eq!(dissect(pattern, "1").expect(""), m);
        assert_eq!(
            dissect(pattern, "one"),
            Err(DissectError::RuntimeError(
                RuntimeError::CannotParseValueToType("one".to_string(), SupportedType::Float)
            ))
        );
    }

    #[test]
    fn type_int() {
        let pattern = "%{f:int}";
        let p = lex(pattern).expect("failed to compile pattern");
        assert_eq!(
            p,
            vec![Token::Field(Field {
                value: "f".to_owned(),
                category: Some(FieldCategory::Typed(SupportedType::Integer))
            })]
        );

        let mut m = HashMap::new();
        m.insert("f".to_string(), Value::Integer(1));

        assert_eq!(
            dissect(pattern, "1.0"),
            Err(DissectError::RuntimeError(
                RuntimeError::CannotParseValueToType("1.0".to_string(), SupportedType::Integer)
            ))
        );

        assert_eq!(dissect(pattern, "1").expect(""), m);

        assert_eq!(
            dissect(pattern, "one"),
            Err(DissectError::RuntimeError(
                RuntimeError::CannotParseValueToType("one".to_string(), SupportedType::Integer)
            ))
        );
    }

    #[test]
    pub fn parse_errors_happen_at_right_places_with_correct_messages() {
        let combinations = [
            ("%{foo", "The { at 0 has no matching }"),
            ("%{foo}%{bar}", "No delimiter between tokens at 6"),
            ("%{foo}%{_(@}", "Invalid padding (@ at 6"),
            ("%{?}", "Invalid token ? at 0"),
            ("%{foo:blah}", "Type blah not supported"),
            (
                "%{foo}|%{+bar:int}",
                "Can't use types with append for field bar",
            ),
        ];

        combinations.iter().for_each(|c| match lex(c.0) {
            Ok(x) => assert_eq!(x, vec![]),
            Err(e) => match e {
                DissectError::ParseError(p) => assert_eq!(p.to_string(), c.1),
                _ => {}
            },
        });
    }

    #[test]
    pub fn runtime_errors_happen_at_right_places_with_correct_messages() {
        let combinations = [
            ("%{foo}|", "bar", "Missing delimiter | in the input"),
            ("%{&foo}", "John", "named field foo not found"),
            (
                "%{foo} ",
                "foo bar baz",
                "Input not completely parsed starting at 4",
            ),
            (
                "%{foo} %{bar}",
                "foo ",
                "Tokens still remaining to be parsed from 2",
            ),
        ];

        combinations.iter().for_each(|c| match dissect(c.0, c.1) {
            Ok(x) => assert_eq!(x, HashMap::new()),
            Err(e) => match e {
                DissectError::RuntimeError(p) => assert_eq!(p.to_string(), c.2),
                _ => {}
            },
        });
    }
}
