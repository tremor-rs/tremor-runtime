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

use super::*;

fn p(s: &str) -> Token {
    Token::Field(s.into())
}

pub fn d(s: &str) -> Token {
    (Token::Delimiter(s.to_string()))
}

pub fn v(s: &[(&str, &str)]) -> HashMap<String, Value> {
    s.iter()
        .map(|(x, y)| (x.to_string(), Value::String(y.to_string())))
        .collect()
}

pub fn pad(s: &str) -> Token {
    Token::Padding(s.to_string())
}

#[test]
fn empty() {
    assert_eq!(lex("").expect("failed to compile pattern"), vec![]);
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
    m.insert("f".to_string(), Value::Float(1.0));

    let l = lex(p).expect("");
    assert_eq!(
        l,
        vec![Token::Field(Field {
            value: "f".to_owned(),
            category: Some(FieldCategory::Typed(SupportedType::Float))
        })]
    );

    assert_eq!(dissect(p, "1.0"), Ok(m.clone()));
    assert_eq!(dissect(p, "1"), Ok(m));
    assert_eq!(
        dissect(p, "one"),
        Err(DissectError::RuntimeError(
            RuntimeError::CannotParseValueToType("one".to_string(), SupportedType::Float)
        ))
    );
}

#[test]
fn type_int() {
    let p = "%{i: int}";
    let mut m = HashMap::new();
    m.insert("i".to_string(), Value::Integer(1));

    let i = lex(p).expect("");
    assert_eq!(
        i,
        vec![Token::Field(Field {
            value: "i".to_owned(),
            category: Some(FieldCategory::Typed(SupportedType::Integer))
        })]
    );

    assert_eq!(
        dissect(p, "1.0"),
        Err(DissectError::RuntimeError(
            RuntimeError::CannotParseValueToType("1.0".to_owned(), SupportedType::Integer)
        ))
    );
    assert_eq!(dissect(p, "1"), Ok(m));
    assert_eq!(
        dissect(p, "one"),
        Err(DissectError::RuntimeError(
            RuntimeError::CannotParseValueToType("one".to_string(), SupportedType::Integer)
        ))
    );
}

#[test]
fn type_bad_type() {
    assert_eq!(
        lex("ints is not a type %{test:ints}"),
        Err(DissectError::ParseError(ParseError::TypeNotSupported(
            "ints".to_string()
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
                value: "name".to_string(),
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
    m.insert("name".to_string(), Value::String("test".to_string()));
    assert_eq!(dissect(p, "this is a test case").expect(""), m);
}

#[test]
fn do_extract2() {
    let p = "this is a %{what} case named %{name}";
    let mut m = HashMap::new();
    m.insert("what".to_string(), Value::String("test".to_string()));
    m.insert("name".to_string(), Value::String("cake".to_string()));
    assert_eq!(dissect(p, "this is a test case named cake").expect(""), m);
}

#[test]
fn do_extract_with_padding() {
    let p = "this is a %{what}%{_}case named %{name}";
    let mut m = HashMap::new();
    m.insert("what".to_string(), Value::String("test".to_string()));
    m.insert("name".to_string(), Value::String("cake".to_string()));
    assert_eq!(
        dissect(p, "this is a test      case named cake").expect(""),
        m
    );
}

#[test]
fn two_pads() {
    let p = "%{_}this%{_}%{_(-)}works";
    let m = HashMap::new();
    assert_eq!(dissect(p, "this     -----works").expect(""), m)
}

#[test]
fn middle_pads_w_delim() {
    let p = "|%{n}%{_}|";
    let mut m = HashMap::new();
    m.insert("n".to_string(), Value::String("Jim".into()));
    assert_eq!(dissect(p, "|Jim |").expect(""), m);
    let mut m = HashMap::new();
    m.insert("n".to_string(), Value::String("John".to_owned()).into());
    assert_eq!(dissect(p, "|John|").expect(""), m);
}

#[test]
fn middle_pads() {
    let p = "%{a}%{_}%{b}";
    let mut m = HashMap::new();
    m.insert("a".to_string(), Value::String("this".to_string()));
    m.insert("b".to_string(), Value::String("works".to_string()));
    assert_eq!(dissect(p, "this     works").expect(""), m)
}

#[test]
fn left_pads() {
    let p = "%{_}%{b}";
    let mut m = HashMap::new();
    m.insert("b".to_string(), Value::String("works".to_string()));
    assert_eq!(dissect(p, "     works").expect(""), m);
}

#[test]
fn right_pads() {
    let p = "%{a}%{_}";
    let mut m = HashMap::new();
    m.insert("a".to_string(), Value::String("this".to_owned()));
    assert_eq!(dissect(p, "this     ").expect(""), m);
}

#[test]
fn right_pads_last_opt() {
    let p = "%{a}%{_}";
    let mut m = HashMap::new();
    m.insert("a".to_string(), Value::String("this".to_owned()));
    assert_eq!(dissect(p, "this").expect(""), m.clone());
    assert_eq!(dissect(p, "this ").expect(""), m.clone());
    assert_eq!(dissect(p, "this   ").expect(""), m.clone());
}

#[test]
fn right_pads_last() {
    let p = "%{a}%{_}";
    let mut m = HashMap::new();
    m.insert("a".to_string(), Value::String("this".to_owned()));
    assert_eq!(dissect(p, "this ").expect(""), m.clone());
    assert_eq!(dissect(p, "this   ").expect(""), m);
}
#[test]
fn do_extract_with_padding_specific() {
    let p = "this is a %{what}%{_( case)} named %{name}";
    let mut m = HashMap::new();
    m.insert("what".to_string(), Value::String("test".to_owned()));
    m.insert("name".to_string(), Value::String("cake".to_owned()));
    assert_eq!(dissect(p, "this is a test case named cake").expect(""), m)
}

#[test]
fn do_extract_ignore() {
    let p = "this is a %{?what} case named %{name}";
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String("cake".to_owned()));
    assert_eq!(dissect(p, "this is a test case named cake").expect(""), m);
}

#[test]
fn do_kv() {
    let p = "this is a %{?name} case named %{&name}";
    let mut m = HashMap::new();
    m.insert("test".to_string(), Value::String("cake".to_string()));
    assert_eq!(dissect(p, "this is a test case named cake").expect(""), m)
}

#[test]
fn do_repeat_kv() {
    let p = "%{?count}: %{&count:int}, %{?count}: %{&count:int}";
    let mut m = HashMap::new();
    m.insert("logstash".to_string(), Value::Integer(0));
    m.insert("tremor".to_string(), Value::Integer(1));
    assert_eq!(dissect(p, "tremor: 1, logstash: 0").expect(""), m)
}

/*
#[test]
fn do_arr() {
    let p = "this is a %{^arr} case named %{^arr}");;
    let mut m = HashMap::new();
    m.insert(
        "arr".to_string(),
        Value::Array(vec![Value::String("test"), Value::String("cake")]),
    );
    assert_eq!(p.run("this is a test case named cake"), m)
}

#[test]
fn do_arr_upgrade() {
    let p = lex("this is a %{arr} case named %{^arr}").expect("failed to compile pattern");
    let mut m = HashMap::new();
    m.insert(
        "arr".to_string(),
        Value::Array(vec![Value::String("test"), Value::String("cake")]),
    );
    assert_eq!(p.run("this is a test case named cake"), m)
}
*/

#[test]
fn do_str() {
    let p = "this is a %{arr} %{+arr}";
    let mut m = HashMap::new();
    m.insert("arr".to_string(), Value::String("test cake".to_string()));
    assert_eq!(dissect(p, "this is a test cake").expect(""), m);
}

#[test]
fn do_str_error1() {
    let p = "this is a %{arr} %{+arr:int}";
    assert_eq!(
        lex(p),
        Err(DissectError::ParseError(
            ParseError::AppendDoesNotSupportTypes("arr".to_owned(), 17)
        ))
    );
}

#[test]
fn do_str_error2() {
    let p = dissect("this is a %{arr:int} %{+arr}", "this is a 12 cake");
    assert_eq!(
        p,
        Err(DissectError::ParseError(
            ParseError::AppendDoesNotSupportTypes("arr".to_owned(), 21)
        ))
    );
}

#[test]
fn do_str_ts() {
    let p = "%{}>%{+syslog_timestamp} %{+syslog_timestamp} %{+syslog_timestamp} %{syslog_hostname} %{syslog_program}: %{full_message}";
    let mut m = HashMap::new();
    m.insert(
        "syslog_timestamp".to_string(),
        Value::String("2019-04-26 14:34 UTC+1".to_string()),
    );
    m.insert(
        "syslog_hostname".to_string(),
        Value::String("tremor.local".to_string()),
    );
    m.insert(
        "syslog_program".to_string(),
        Value::String("tremor".to_string()),
    );
    m.insert(
        "full_message".to_string(),
        Value::String("can do concat!".to_string()),
    );
    assert_eq!(
        dissect(
            p,
            "INFO>2019-04-26 14:34 UTC+1 tremor.local tremor: can do concat!"
        )
        .expect(""),
        m
    )
}

#[test]
fn dissect_multiple_padding_in_the_middle() {
    let p = "%{name}%{_}%{_(|)}%{age}";
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String("John".to_string()));
    m.insert("age".to_string(), Value::String("22".to_string()));
    let input = "John  ||22";
    assert_eq!(dissect(p, input).expect(""), m)
}

#[test]
fn dissect_multiple_padding_in_the_middle_and_delim_err() {
    let p = "%{name}%{_}%{_(|)}/ %{age}";
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String("John".to_string()));
    m.insert("age".to_string(), Value::String("22".to_string()));
    let input = "John |/ 22";
    assert_eq!(dissect(p, input).expect(""), m);
}

#[test]
fn dissect_multiple_padding_in_the_middle_and_delim_ok() {
    let p = "%{name}%{_}%{_(|)}/%{age}";
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String("John".to_string()));
    m.insert("age".to_string(), Value::String("22".to_string()));
    let input = "John/22";
    assert_eq!(dissect(p, input).expect(""), m);
}

#[test]
fn dissect_multiple_padding_in_the_middle_and_delim_2() {
    let p = "%{name}%{_}%{_(|)}/%{age}";
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String("John".to_owned()));
    m.insert("age".to_string(), Value::String("22".to_owned()));
    let input = "John /22";
    assert_eq!(dissect(p, input).expect(""), m)
}

#[test]
fn dissect_multiple_padding_in_the_middle_and_delim_3() {
    let p = "%{name}%{_}%{_(|)}/%{age}";
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String("John".to_string()));
    m.insert("age".to_string(), Value::String("22".to_string()));
    let input = "John|/22";
    assert_eq!(dissect(p, input).expect(""), m)
}

#[test]
fn dissect_multiple_padding_in_the_middle_and_delim_4() {
    let p = "%{name}%{_}%{_(|)}/%{age}";
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String("John".to_string()));
    m.insert("age".to_string(), Value::String("22".to_string()));
    let input = "John |/22";
    assert_eq!(dissect(p, input).expect(""), m)
}

#[test]
fn dissect_multiple_padding_in_the_middle_first_not_found() {
    let p = "%{name}%{_}%{_(|)}%{age}";
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String("John".to_string()));
    m.insert("age".to_string(), Value::String("22".to_string()));
    let input = "John||22";
    assert_eq!(dissect(p, input).expect(""), m);
}

#[test]
fn dissect_multiple_padding_in_the_middle_last_not_found() {
    let p = "%{name}%{_}%{_(|)}%{age}";
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String("John".to_owned()));
    m.insert("age".to_string(), Value::String("22".to_owned()));
    let input = "John 22";
    assert_eq!(dissect(p, input).expect(""), m);
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
        let output = dissect(pattern, input).expect("");
        assert_eq!(output, *expected);
    });
}
