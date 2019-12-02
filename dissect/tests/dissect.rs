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

use dissect::*;
use simd_json::value::borrowed::{Object, Value};

fn cp(pattern: &str) -> Pattern {
    Pattern::compile(pattern).expect("failed to compile pattern")
}
fn run(pattern: &str, input: &str) -> Option<Object<'static>> {
    cp(pattern).run(input)
}

fn v<'dissect, T: Copy>(s: &'dissect [(&str, T)]) -> Option<Object<'dissect>>
where
    Value<'dissect>: From<T>,
{
    use std::borrow::Cow;
    Some(
        s.into_iter()
            .map(|(x, y)| (Into::<Cow<'dissect, str>>::into(*x), Value::from(*y)))
            .collect(),
    )
}

macro_rules! assert_pattern {
    ($pattern:expr, $input:expr) => {
        assert_eq!(run($pattern, $input), None)
    };
    ($pattern:expr, $input:expr, $($args:expr),*) => {
        assert_eq!(run($pattern, $input), v(&[$($args),*]))
    };
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

/* Old tests


#[cfg(test)]
mod tests {
    use super::*;
    use simd_json::borrowed::Value as SimdValue;

    fn p(s: &str) -> Token {
        Token::Field(s.into())
    }

    fn d(s: &str) -> Token {
        (Token::Delimiter(s.to_string()))
    }

    fn v<'dissect>(s: &'dissect [(&str, &str)]) -> Dissect<'dissect> {
        Dissect(
            s.into_iter()
                .map(|(x, y)| {
                    (
                        Into::<Cow<'dissect, str>>::into(*x),
                        SimdValue::String(y.to_string().into()),
                    )
                })
                .collect(),
        )
    }

    fn pad(s: &str) -> Token {
        Token::Padding(s.to_string())
    }

    #[test]
    fn dissect_multiple_padding() {
        let pattern = "%{name}%{_}%{_(|)}";
        let input = "John  ||";
        assert_eq!(
            Pattern::try_from(pattern)
                .expect("")
                .extract(input)
                .expect(""),
            v(&[("name", "John")])
        );
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle_1() {
        let pattern = "%{name}%{_}%{_(|)}%{age}";
        let input = "John  ||22";
        assert_eq!(
            Pattern::try_from(pattern)
                .expect("")
                .extract(input)
                .expect(""),
            v(&[("name", "John"), ("age", "22")])
        );
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle_with_delim_err() {
        let pattern = "%{name}%{_}%{_(|)}/ %{age}";
        let input = "John |/ 22";
        let output = Pattern::try_from(pattern).expect("");
        let output = output.extract(input).expect("");
        assert_eq!(output, v(&([("name", "John"), ("age", "22")])));
    }

    #[test]
    fn dissect_skipped_fields() {
        let pattern = "%{?first_name} %{last_name}";
        let input = "John Doe";
        assert_eq!(
            Pattern::try_from(pattern)
                .expect("")
                .extract(input)
                .expect("skipped fields doesn't work"),
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
        let output = Pattern::try_from(pattern).expect("");
        let output = output
            .extract(input)
            .expect("plain skipped field doesnt work");
        assert_eq!(output, Dissect(HashMap::new()));
    }

    #[test]
    fn dissect_multiple_skipped_fields() {
        let pattern = "%{?first_name} %{?middle_name} %{last_name}";
        let input = "John Michael Doe";
        let output = Pattern::try_from(pattern).expect("");
        let output = output
            .extract(input)
            .expect("multiple skipped fields doesn't work");
        assert_eq!(output, v(&[("last_name", "Doe")]));
    }

    #[test]
    fn dissect_skipped_field_at_eol() {
        let pattern = "%{first_name} %{?last_name}";
        let input = "John Doe";
        let output = Pattern::try_from(pattern).expect("");
        let output = output
            .extract(input)
            .expect("dissect doesn't skip fields at end of line");
        assert_eq!(output, v(&[("first_name", "John")]));
    }

    #[test]
    fn dissect_with_integer_type() {
        let pattern = "%{name} %{age:int}";
        let input = "John 22";
        let output = Pattern::try_from(pattern).expect("");
        let output = output.extract(input).expect("dissect doesn't do types");
        let mut expected = HashMap::new();
        expected.insert("name".into(), SimdValue::String("John".into()));
        expected.insert("age".into(), SimdValue::from(22));
        assert_eq!(output, Dissect(expected));
    }

    #[test]
    fn dissect_with_float_type() {
        let pattern = "%{name} %{tax:float}";
        let input = "John 414.203";
        let output = Pattern::try_from(pattern).expect("");
        let output = output.extract(input).expect("dissect doesn't float");

        assert_eq!(
            output.0.get("name".into()),
            Some(&SimdValue::String("John".into()))
        );
        match output.0.get("tax".into()) {
            Some(SimdValue::from(f)) => assert!((f - 414.203).abs() < 0.001),
            _ => unreachable!(),
        };
    }

    #[test]
    fn dissect_with_string_is_a_noop() {
        let pattern = "%{name:string}";
        let input = "John";
        let output = Pattern::try_from(pattern).expect("");
        let output = output.extract(input).expect(
            "dissect doesn't work with string as a type even though it is a string already",
        );
        let mut expected = HashMap::new();
        expected.insert("name".into(), SimdValue::String("John".into()));

        assert_eq!(output, Dissect(expected));
    }

    #[test]
    fn dissect_with_wrong_type_errors_out() {
        let pattern = "%{name} %{age:wakanda}";
        let output = Pattern::try_from(pattern);
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
        let output = Pattern::try_from(pattern).expect("");
        let output = output
            .extract(input)
            .expect("dissect doesn't like empty fields");
        assert_eq!(output, v(&[("name", "John")]));
    }

    #[test]
    fn dissect_append_field() {
        let pattern = "%{+name} %{+name} %{age}";
        let input = "John Doe 22";
        let output = Pattern::try_from(pattern).expect("");
        let output = output.extract(input).expect("dissect doesn't like append");
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
                    value: "name".into(),
                    category: Some(FieldCategory::Append("name".into()))
                }),
                Token::Delimiter(" ".to_string()),
                Token::Field(Field {
                    value: "name".into(),
                    category: Some(FieldCategory::Append("name".into()))
                })
            ]
        );
    }

    #[test]
    fn dissect_simple_append() {
        let pattern = "%{+name} %{+name}";
        let input = "John Doe";
        let output = Pattern::try_from(pattern).expect("");
        let output = output.extract(input).expect("dissect doesn't append");
        assert_eq!(output, v(&[("name", "John Doe")]));
    }

    #[test]
    fn dissect_out_of_order_append() {
        let pattern = "%{+name} %{age} %{+name}";
        let input = "John 22 Doe";
        let output = Pattern::try_from(pattern).expect("");
        let output = output.extract(input).expect("out of order");
        assert_eq!(output, v(&[("name", "John Doe"), ("age", "22")]));
    }

    #[test]
    fn dissect_multiple_append() {
        let pattern = "%{+name} %{+country}|%{+name} %{+country}";
        let input = "John United|Doe States";
        let output = Pattern::try_from(pattern).expect("");

        let output = output.extract(input).expect("doesn't work with 2 appends");
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
                ParseError::AppendDoesNotSupportTypes("age".into(), 0)
            ))
        );
    }

    #[test]
    fn lex_fails_for_append_with_type_on_append() {
        let pattern = "%{age} %{+age:int}";
        let output = Pattern::try_from(pattern);
        assert_eq!(
            output,
            Err(DissectError::ParseError(
                ParseError::AppendDoesNotSupportTypes("age".into(), 7)
            ))
        )
    }

    #[test]
    fn lex_fails_for_append_with_type_on_non_append() {
        let pattern = "%{age:int} %{+age}";

        let output = Pattern::try_from(pattern);
        assert_eq!(
            output,
            Err(DissectError::ParseError(
                ParseError::AppendDoesNotSupportTypes("age".into(), 11)
            ))
        );
    }





    #[test]
    fn dissect_empty() {
        let output = Pattern::try_from("").expect("");
        let output = output.extract("").expect("");
        assert_eq!(output, Dissect(HashMap::new()));
    }

    #[test]
    fn multi_pad() {
        let pattern = "|%{n}%{_}|";
        let input = "|foo |";
        let pt = Pattern::try_from(pattern).expect("cannot parse pattern");

        let output = pt.extract(input).expect("");
        let second_op = pt.extract("|foo|").expect("");

        assert_eq!(output, v(&[("n", "foo")]));
        assert_eq!(second_op, v(&[("n", "foo")]));
    }

    #[test]
    fn extract_with_padding_specific() {
        let pattern = "this is a %{what}%{_( case)} named %{name}";
        let input = "this is a test case named cake";
        let output = Pattern::try_from(pattern).expect("cannot parse pattern");
        let output = output.extract(input).expect("");

        assert_eq!(output, v(&[("what", "test"), ("name", "cake")]));
    }

    #[test]
    fn do_kv_1() {
        let pattern = "this is a %{?name} case named %{&name}";
        let input = "this is a test case named cake";
        let output = Pattern::try_from(pattern).expect("cannot parse pattern");
        let output = output.extract(input).expect("");
        assert_eq!(output, v(&[("test", "cake")]));
    }

    #[test]
    fn do_repeat_kv_1() {
        let pattern = "%{?count}: %{&count:int}, %{?count}: %{&count:int}";
        let input = "tremor: 1, logstash: 0";
        let output = Pattern::try_from(pattern).expect("cannot parse pattern");
        let output = output.extract(input).expect("");
        let mut expected = HashMap::new();
        expected.insert("tremor".into(), SimdValue::from(1));
        expected.insert("logstash".into(), SimdValue::from(0));
        assert_eq!(output, Dissect(expected));
    }

    #[test]
    fn parsing_wrong_value_returns_error() {
        let pattern = "%{foo:int}";
        let input = "abcdef";
        let p = Pattern::try_from(pattern).expect("cannot parse pattern");
        let output = p.extract(input);

        assert_eq!(
            output,
            Err(DissectError::RuntimeError(
                RuntimeError::CannotParseValueToType("abcdef".to_owned(), SupportedType::Integer),
            )),
        );
    }

    #[test]
    fn consecutive_padding_first_one_optional() {
        let pattern = "%{name}%{_}%{_(|)}%{age}";
        let input = "John|22";
        let p = Pattern::try_from(pattern).expect("cannot parse pattern");
        let output = p.extract(input).expect("");
        let mut expected = HashMap::new();
        expected.insert("name".into(), SimdValue::String("John".into()));
        expected.insert("age".into(), SimdValue::String("22".into()));
        assert_eq!(output, Dissect(expected));
    }

    #[test]
    fn empty() {
        assert_eq!(lex("").expect("failed to compile pattern"), vec![]);
    }

    #[test]
    fn parse_errors_happen_at_right_places_with_correct_messages() {
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
    fn runtime_errors_happen_at_right_places_with_correct_messages() {
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

        combinations
            .iter()
            .for_each(|c| match Pattern::try_from(c.0).expect("").extract(c.1) {
                Ok(x) => assert_eq!(x, Dissect(HashMap::new())),
                Err(e) => match e {
                    DissectError::RuntimeError(p) => assert_eq!(p.to_string(), c.2),
                    _ => {}
                },
            });
    }

    #[test]
    fn extract() {
        let output = lex("%{test}").expect("failed to compile pattern");

        assert_eq!(output, vec![p("test")]);
    }

    #[test]
    fn type_float() {
        let p = "%{f:float}";
        let mut m = HashMap::new();
        m.insert("f".into(), SimdValue::from(1.0));

        let pattern = Pattern::try_from(p).expect("can parse token");
        assert_eq!(
            pattern,
            Pattern {
                tokens: vec![Token::Field(Field {
                    value: "f".into(),
                    category: Some(FieldCategory::Typed(SupportedType::Float))
                })]
            }
        );

        assert_eq!(
            Pattern::try_from(p).expect("").extract("1.0"),
            Ok(Dissect(m.clone()))
        );
        assert_eq!(Pattern::try_from(p).expect("").extract("1"), Ok(Dissect(m)));
        assert_eq!(
            Pattern::try_from(p).expect("").extract("one"),
            Err(DissectError::RuntimeError(
                RuntimeError::CannotParseValueToType("one".into(), SupportedType::Float)
            ))
        );
    }

    #[test]
    fn type_int() {
        let p = "%{i: int}";
        let mut m = HashMap::new();
        m.insert("i".into(), SimdValue::from(1));

        let i = lex(p).expect("");
        assert_eq!(
            i,
            vec![Token::Field(Field {
                value: "i".into(),
                category: Some(FieldCategory::Typed(SupportedType::Integer))
            })]
        );

        assert_eq!(
            Pattern::try_from(p).expect("").extract("1.0"),
            Err(DissectError::RuntimeError(
                RuntimeError::CannotParseValueToType("1.0".into(), SupportedType::Integer)
            ))
        );
        assert_eq!(Pattern::try_from(p).expect("").extract("1"), Ok(Dissect(m)));
        assert_eq!(
            Pattern::try_from(p).expect("").extract("one"),
            Err(DissectError::RuntimeError(
                RuntimeError::CannotParseValueToType("one".into(), SupportedType::Integer)
            ))
        );
    }

    #[test]
    fn type_bad_type() {
        assert_eq!(
            lex("ints is not a type %{test:ints}"),
            Err(DissectError::ParseError(ParseError::TypeNotSupported(
                "ints".into()
            )))
        );
    }
    #[test]
    fn prefix() {
        assert_eq!(
            lex("this is %{test}").expect("failed to compile pattern"),
            vec![d("this is "), p("test")]
        );
    }

    #[test]
    fn suffix() {
        assert_eq!(
            lex("%{test} case").expect("failed to compile pattern"),
            vec![p("test"), d(" case")]
        );
    }

    #[test]
    fn encircled() {
        assert_eq!(
            lex("this is %{test} case").expect("failed to compile pattern"),
            vec![d("this is "), p("test"), d(" case")]
        );
    }

    #[test]
    fn two_patterns() {
        assert_eq!(
            lex("this is %{test} case named %{name}").expect("failed to compile pattern"),
            vec![d("this is "), p("test"), d(" case named "), p("name")]
        );
    }

    #[test]
    fn two_ignore() {
        assert_eq!(
            lex("this is %{test} case named %{?name}").expect("failed to compile pattern"),
            vec![
                d("this is "),
                p("test"),
                d(" case named "),
                Token::Field(Field {
                    value: "name".into(),
                    category: Some(FieldCategory::Skipped)
                })
            ]
        );
    }

    #[test]
    fn open_extract() {
        assert_eq!(
            lex("%{test"),
            Err(DissectError::ParseError(ParseError::MissingClosingBracket(
                0
            )))
        );
    }

    #[test]
    fn open_middle() {
        assert_eq!(
            lex("%{test %{case}"),
            Err(DissectError::ParseError(ParseError::MissingClosingBracket(
                0
            )))
        );
    }

    #[test]
    fn extract_inside() {
        assert_eq!(
            lex("this is %{test case"),
            Err(DissectError::ParseError(ParseError::MissingClosingBracket(
                8
            )))
        );
    }

    #[test]
    fn connected_extract() {
        assert_eq!(
            lex("%{test}%{pattern}"),
            Err(DissectError::ParseError(ParseError::NoDelimiter(7)))
        );
    }

    #[test]
    fn do_extract1() {
        let p = "this is a %{name} case";
        let mut m = HashMap::new();
        m.insert("name".into(), SimdValue::String("test".into()));
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("this is a test case")
                .expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn do_extract2() {
        let p = "this is a %{what} case named %{name}";
        let mut m = HashMap::new();
        m.insert("what".into(), SimdValue::String("test".into()));
        m.insert("name".into(), SimdValue::String("cake".into()));
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("this is a test case named cake")
                .expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn do_extract_with_padding() {
        let p = "this is a %{what}%{_}case named %{name}";
        let mut m = HashMap::new();
        m.insert("what".into(), SimdValue::String("test".into()));
        m.insert("name".into(), SimdValue::String("cake".into()));
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("this is a test      case named cake")
                .expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn two_pads() {
        let p = "%{_}this%{_}%{_(-)}works";
        let m = HashMap::new();
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("this     -----works")
                .expect(""),
            Dissect(m)
        )
    }

    #[test]
    fn middle_pads_w_delim() {
        let p = "|%{n}%{_}|";
        let mut m = HashMap::new();
        m.insert("n".into(), SimdValue::String("Jim".into()));
        assert_eq!(
            Pattern::try_from(p).expect("").extract("|Jim |").expect(""),
            Dissect(m)
        );
        let mut m = HashMap::new();
        m.insert("n".into(), SimdValue::String("John".into()).into());
        assert_eq!(
            Pattern::try_from(p).expect("").extract("|John|").expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn middle_pads() {
        let p = "%{a}%{_}%{b}";
        let mut m = HashMap::new();
        m.insert("a".into(), SimdValue::String("this".into()));
        m.insert("b".into(), SimdValue::String("works".into()));
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("this     works")
                .expect(""),
            Dissect(m)
        )
    }

    #[test]
    fn left_pads() {
        let p = "%{_}%{b}";
        let mut m = HashMap::new();
        m.insert("b".into(), SimdValue::String("works".into()));
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("     works")
                .expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn right_pads() {
        let p = "%{a}%{_}";
        let mut m = HashMap::new();
        m.insert("a".into(), SimdValue::String("this".into()));
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("this     ")
                .expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn right_pads_last_opt() {
        let p = "%{a}%{_}";
        let mut m = HashMap::new();
        m.insert("a".into(), SimdValue::String("this".into()));
        let m = Dissect(m);
        assert_eq!(
            &Pattern::try_from(p).expect("").extract("this").expect(""),
            &m
        );
        assert_eq!(
            &Pattern::try_from(p).expect("").extract("this ").expect(""),
            &m
        );
        assert_eq!(
            &Pattern::try_from(p)
                .expect("")
                .extract("this   ")
                .expect(""),
            &m
        );
    }

    #[test]
    fn right_pads_last() {
        let p = "%{a}%{_}";
        let mut m = HashMap::new();
        m.insert("a".into(), SimdValue::String("this".into()));
        let p = Pattern::try_from(p).expect("");
        assert_eq!(p.extract("this ").expect(""), Dissect(m.clone()));
        assert_eq!(p.extract("this   ").expect(""), Dissect(m));
    }
    #[test]
    fn do_extract_with_padding_specific() {
        let p = "this is a %{what}%{_( case)} named %{name}";
        let mut m = HashMap::new();
        m.insert("what".into(), SimdValue::String("test".into()));
        m.insert("name".into(), SimdValue::String("cake".into()));
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("this is a test case named cake")
                .expect(""),
            Dissect(m)
        )
    }

    #[test]
    fn do_extract_ignore() {
        let p = "this is a %{?what} case named %{name}";
        let mut m = HashMap::new();
        m.insert("name".into(), SimdValue::String("cake".into()));
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("this is a test case named cake")
                .expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn do_kv() {
        let p = "this is a %{?name} case named %{&name}";
        let mut m = HashMap::new();
        m.insert("test".into(), SimdValue::String("cake".into()));
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("this is a test case named cake")
                .expect(""),
            Dissect(m)
        )
    }

    #[test]
    fn do_repeat_kv() {
        let p = "%{?count}: %{&count:int}, %{?count}: %{&count:int}";
        let mut m = HashMap::new();
        m.insert("logstash".into(), SimdValue::from(0));
        m.insert("tremor".into(), SimdValue::from(1));
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("tremor: 1, logstash: 0")
                .expect(""),
            Dissect(m)
        )
    }

    /*
    #[test]
    fn do_arr() {
        let p = "this is a %{^arr} case named %{^arr}");
        let mut m = HashMap::new();
        m.insert(
            "arr".into(),
            Value::Array(vec![Value::String("test"), SimdValue::String("cake")]),
        );
        assert_eq!(p.run("this is a test case named cake"), Dissect(m))
    }

    #[test]
    fn do_arr_upgrade() {
        let p = lex("this is a %{arr} case named %{^arr}").expect("failed to compile pattern");
        let mut m = HashMap::new();
        m.insert(
            "arr".into(),
            Value::Array(vec![Value::String("test"), SimdValue::String("cake")]),
        );
        assert_eq!(p.run("this is a test case named cake"), Dissect(m))
    }
    */

    #[test]
    fn do_str() {
        let p = "this is a %{arr} %{+arr}";
        let mut m = HashMap::new();
        m.insert("arr".into(), SimdValue::String("test cake".into()));
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("this is a test cake")
                .expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn do_str_error1() {
        let p = "this is a %{arr} %{+arr:int}";
        assert_eq!(
            lex(p),
            Err(DissectError::ParseError(
                ParseError::AppendDoesNotSupportTypes("arr".into(), 17)
            ))
        );
    }

    #[test]
    fn do_str_error2() {
        let p = Pattern::try_from("this is a %{arr:int} %{+arr}");

        assert_eq!(
            p,
            Err(DissectError::ParseError(
                ParseError::AppendDoesNotSupportTypes("arr".into(), 21)
            ))
        );
    }

    #[test]
    fn do_str_ts() {
        let p = "%{}>%{+syslog_timestamp} %{+syslog_timestamp} %{+syslog_timestamp} %{syslog_hostname} %{syslog_program}: %{full_message}";
        let mut m = HashMap::new();
        m.insert(
            "syslog_timestamp".into(),
            SimdValue::String("2019-04-26 14:34 UTC+1".into()),
        );
        m.insert(
            "syslog_hostname".into(),
            SimdValue::String("tremor.local".into()),
        );
        m.insert("syslog_program".into(), SimdValue::String("tremor".into()));
        m.insert(
            "full_message".into(),
            SimdValue::String("can do concat!".into()),
        );
        assert_eq!(
            Pattern::try_from(p)
                .expect("")
                .extract("INFO>2019-04-26 14:34 UTC+1 tremor.local tremor: can do concat!")
                .expect(""),
            Dissect(m)
        )
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle() {
        let p = "%{name}%{_}%{_(|)}%{age}";
        let mut m = HashMap::new();
        m.insert("name".into(), SimdValue::String("John".into()));
        m.insert("age".into(), SimdValue::String("22".into()));
        let input = "John  ||22";
        assert_eq!(
            Pattern::try_from(p).expect("").extract(input).expect(""),
            Dissect(m)
        )
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle_and_delim_err() {
        let p = "%{name}%{_}%{_(|)}/ %{age}";
        let mut m = HashMap::new();
        m.insert("name".into(), SimdValue::String("John".into()));
        m.insert("age".into(), SimdValue::String("22".into()));
        let input = "John |/ 22";
        assert_eq!(
            Pattern::try_from(p).expect("").extract(input).expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle_and_delim_ok() {
        let p = "%{name}%{_}%{_(|)}/%{age}";
        let mut m = HashMap::new();
        m.insert("name".into(), SimdValue::String("John".into()));
        m.insert("age".into(), SimdValue::String("22".into()));
        let input = "John/22";
        assert_eq!(
            Pattern::try_from(p).expect("").extract(input).expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle_and_delim_2() {
        let p = "%{name}%{_}%{_(|)}/%{age}";
        let mut m = HashMap::new();
        m.insert("name".into(), SimdValue::String("John".into()));
        m.insert("age".into(), SimdValue::String("22".into()));
        let input = "John /22";
        assert_eq!(
            Pattern::try_from(p).expect("").extract(input).expect(""),
            Dissect(m)
        )
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle_and_delim_3() {
        let p = "%{name}%{_}%{_(|)}/%{age}";
        let mut m = HashMap::new();
        m.insert("name".into(), SimdValue::String("John".into()));
        m.insert("age".into(), SimdValue::String("22".into()));
        let input = "John|/22";
        assert_eq!(
            Pattern::try_from(p).expect("").extract(input).expect(""),
            Dissect(m)
        )
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle_and_delim_4() {
        let p = "%{name}%{_}%{_(|)}/%{age}";
        let mut m = HashMap::new();
        m.insert("name".into(), SimdValue::String("John".into()));
        m.insert("age".into(), SimdValue::String("22".into()));
        let input = "John |/22";
        assert_eq!(
            Pattern::try_from(p).expect("").extract(input).expect(""),
            Dissect(m)
        )
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle_first_not_found() {
        let p = "%{name}%{_}%{_(|)}%{age}";
        let mut m = HashMap::new();
        m.insert("name".into(), SimdValue::String("John".into()));
        m.insert("age".into(), SimdValue::String("22".into()));
        let input = "John||22";
        assert_eq!(
            Pattern::try_from(p).expect("").extract(input).expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn dissect_multiple_padding_in_the_middle_last_not_found() {
        let p = "%{name}%{_}%{_(|)}%{age}";
        let mut m = HashMap::new();
        m.insert("name".into(), SimdValue::String("John".into()));
        m.insert("age".into(), SimdValue::String("22".into()));
        let input = "John 22";
        assert_eq!(
            Pattern::try_from(p).expect("").extract(input).expect(""),
            Dissect(m)
        );
    }

    #[test]
    fn weblog() {
        let pattern = r#"%{syslog_timestamp} %{syslog_hostname} %{?syslog_prog}: %{syslog_program_aux}[%{syslog_pid:int}] %{request_unix_time} %{request_timestamp} %{request_elapsed_time} %{server_addr}:%{server_port:int} %{remote_addr}:%{remote_port:int} "%{response_content_type}" %{response_content_length} %{request_status} %{bytes_sent} %{request_length} "%{url_scheme}" "%{http_host}" "%{request_method} %{request_url} %{request_protocol}" "%{http_referer}" "%{http_user_agent}" "%{http_x_forwarded_for}" "%{http_ttrue_client_ip}" "%{remote_user}" "%{is_bot}" "%{admin_user}" "%{http_via}" "%{response_location}" "%{set_cookie}" "%{http_cookie}" "%{moawsl_info}" "%{php_message}" "%{akamai_edgescape}" "%{uid_info}" "%{geoip_country}" "%{geoip_region}" "%{geoip_city}" "%{geoip_postal}" "%{geoip_dma}" "%{server_id}" "%{txid}" "%{hpcnt}" "%{client_accept}" "%{client_accept_charset}" "%{client_accept_encoding}" "%{client_accept_language}" "%{client_accept_datetime}" "%{client_pragma}" "%{client_transfer_encoding}" "%{client_attdeviceid}" "%{client_wap_profile}" %{weblog_end}"#;
        let p = lex("%{name}%{_}%{_(|)}%{age}");
        assert!(lex(pattern).is_ok());
        assert!(p.is_ok());
    }

    #[test]
    fn test_patterns() {
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
                       ( "%{syslog_timestamp} %{wf_host} %{syslog_program}: %{syslog_message}",
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
            let p = Pattern::try_from(pattern).expect("");

            let output = p.extract(input).expect("");
            assert_eq!(output, *expected);
        });
    }
}


*/
