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

use super::*;
use matches::assert_matches;
use proptest::prelude::*;

macro_rules! lex_ok {
    ($src:expr, $($span:expr => $token:expr,)*) => {{
        let lexed_tokens: Vec<_> = Lexer::new($src, arena::Index::from(0usize)).filter_map(|t| match t {
            Ok(Spanned { value: t, span: tspan }) if !t.is_ignorable() => Some((t, tspan.start.column(), tspan.end.column())),
            _ => None
        }).collect();
        // locations are 1-based, so we need to add 1 to every loc, end position needs another +1
        let expected_tokens = vec![$({
            ($token, $span.find("~").unwrap() + 1, $span.rfind("~").unwrap() + 2)
        }),*];

        assert_eq!(lexed_tokens, expected_tokens);
    }};
}

#[rustfmt::skip]
#[test]
fn interpolate() {
    lex_ok! {
        r#"  "" "#,
        r#"  ~ "# => Token::DQuote,
        r#"   ~ "# => Token::DQuote,
    };
    lex_ok! {
        r#"  "hello" "#,
        r#"  ~ "# => Token::DQuote,
        r#"   ~~~~~ "# => Token::StringLiteral("hello".into()),
        r#"        ~ "# => Token::DQuote,
    };
    lex_ok! {
        r#"  "hello #{7}" "#,
        r#"  ~ "# => Token::DQuote,
        r#"   ~~~~~~ "# => Token::StringLiteral("hello ".into()),
        r#"         ~~ "# => Token::Interpol,
        r#"           ~ "# => Token::IntLiteral(7),
        r#"            ~ "# => Token::RBrace,
        r#"             ~ "# => Token::DQuote,

    };
    // We can't use `r#""#` for the string since we got a a `"#` in it
    lex_ok! {
        "  \"#{7} hello\" ",
        r#"  ~ "# => Token::DQuote,
        r#"   ~~ "# => Token::Interpol,
        r#"     ~ "# => Token::IntLiteral(7),
        r#"      ~ "# => Token::RBrace,
        r#"       ~~~~~~ "# => Token::StringLiteral(" hello".into()),
        r#"             ~ "# => Token::DQuote,

    };
    lex_ok! {
        r#"  "hello #{ "snot #{7}" }" "#,
        r#"  ~ "# => Token::DQuote,
        r#"   ~~~~~~ "# => Token::StringLiteral("hello ".into()),
        r#"         ~~ "# => Token::Interpol,
        r#"            ~ "# => Token::DQuote,
        r#"             ~~~~~ "# => Token::StringLiteral("snot ".into()),
        r#"                  ~~ "# => Token::Interpol,
        r#"                    ~ "# => Token::IntLiteral(7),
        r#"                     ~ "# => Token::RBrace,
        r#"                      ~ "# => Token::DQuote,
        r#"                        ~ "# => Token::RBrace,
        r#"                         ~ "# => Token::DQuote,
    };
}

#[rustfmt::skip]
#[test]
fn number_parsing() {
    lex_ok! {
    "1_000_000",
    "~~~~~~~~~" => Token::IntLiteral(1_000_000), };
    lex_ok! {
    "1_000_000_",
    "~~~~~~~~~~" => Token::IntLiteral(1_000_000), };

    lex_ok! {
    "100.0000",
    "~~~~~~~~" => Token::FloatLiteral(100.0000, "100.0000".to_string()), };
    lex_ok! {
    "1_00.0_000_",
    "~~~~~~~~~~~" => Token::FloatLiteral(100.0000, "100.0000".to_string()), };

    lex_ok! {
    "0xFFAA00",
    "~~~~~~~~" => Token::IntLiteral(16_755_200), };
    lex_ok! {
    "0xFF_AA_00",
    "~~~~~~~~~~" => Token::IntLiteral(16_755_200), };
}

#[rustfmt::skip]
#[test]
fn paths() {
    lex_ok! {
        "  hello_hahaha8ABC ",
        "  ~~~~~~~~~~~~~~~~ " => Token::Ident("hello_hahaha8ABC".into(), false),
    };
    lex_ok! {
        "  .florp ",
        "  ~ " => Token::Dot,
        "   ~~~~~ " => Token::Ident("florp".into(), false),
    };
    lex_ok! {
        "  $borp ",
        "  ~ " => Token::Dollar,
        "   ~~~~ " => Token::Ident("borp".into(), false),
    };
    lex_ok! {
        "  $`borp`",
        "  ~ " => Token::Dollar,
        "   ~~~~~~ " => Token::Ident("borp".into(), true),
    };
}

#[rustfmt::skip]
#[test]
fn keywords() {
    lex_ok! {
    " let ",
    " ~~~ " => Token::Let, };
    lex_ok! {
        " match ",
        " ~~~~~ " => Token::Match, };
    lex_ok! {
        " case ",
        " ~~~~ " => Token::Case, };
    lex_ok! {
        " of ",
        " ~~ " => Token::Of, };
    lex_ok! {
        " end ",
        " ~~~ " => Token::End, };
    lex_ok! {
        " drop ",
        " ~~~~ " => Token::Drop, };
    lex_ok! {
        " emit ",
        " ~~~~ " => Token::Emit, };
    lex_ok! {
        " event ",
        " ~~~~~ " => Token::Event, };
    lex_ok! {
        " state ",
        " ~~~~~ " => Token::State, };
    lex_ok! {
        " set ",
        " ~~~ " => Token::Set, };
    lex_ok! {
        " each ",
        " ~~~~ " => Token::Each, };
    lex_ok! {
        " intrinsic ",
        " ~~~~~~~~~ " => Token::Intrinsic, };
}

#[rustfmt::skip]
#[test]
fn operators()  {
    lex_ok! {
        " not null ",
        " ~~~ " => Token::Not,
        "     ~~~~ " => Token::Nil,
    };
    lex_ok! {
        " != null ",
        " ~~ " => Token::NotEq,
        "    ~~~~ " => Token::Nil,
    };
    lex_ok! {
        " !1 ",
        " ~  " => Token::BitNot,
        "  ~ " => Token::IntLiteral(1),
    };
    lex_ok! {
        " ! ",
        " ~ " => Token::BitNot,
    };
    lex_ok! {
        " and ",
        " ~~~ " => Token::And,
    };
    lex_ok! {
        " or ",
        " ~~ " => Token::Or,
    };
    lex_ok! {
        " xor ",
        " ~~~ " => Token::Xor,
    };
    lex_ok! {
        " & ",
        " ~ " => Token::BitAnd,
    };
    /* TODO: enable
        lex_ok! {
            " | ",
            " ~ " => Token::BitOr,
        };*/
    lex_ok! {
        " ^ ",
        " ~ " => Token::BitXor,
    };
    lex_ok! {
        " = ",
        " ~ " => Token::Eq,
    };
    lex_ok! {
        " == ",
        " ~~ " => Token::EqEq,
    };
    lex_ok! {
        " != ",
        " ~~ " => Token::NotEq,
    };
    lex_ok! {
        " >= ",
        " ~~ " => Token::Gte,
    };
    lex_ok! {
        " > ",
        " ~ " => Token::Gt,
    };
    lex_ok! {
        " <= ",
        " ~~ " => Token::Lte,
    };
    lex_ok! {
        " < ",
        " ~ " => Token::Lt,
    };
    lex_ok! {
        " >> ",
        " ~~ " => Token::RBitShiftSigned,
    };
    lex_ok! {
        " >>> ",
        " ~~~ " => Token::RBitShiftUnsigned,
    };
    lex_ok! {
        " << ",
        " ~~ " => Token::LBitShift,
    };
    lex_ok! {
        " + ",
        " ~ " => Token::Add,
    };
    lex_ok! {
        " - ",
        " ~ " => Token::Sub,
    };
    lex_ok! {
        " * ",
        " ~ " => Token::Mul,
    };
    lex_ok! {
        " / ",
        " ~ " => Token::Div,
    };
    lex_ok! {
        " % ",
        " ~ " => Token::Mod,
    };
    
}

#[test]
fn should_disambiguate() {
    // Starts with ':'
    lex_ok! {
        " : ",
        " ~ " => Token::Colon,
    };
    lex_ok! {
        " :: ",
        " ~~ " => Token::ColonColon,
    };

    // Starts with '-'
    lex_ok! {
        " - ",
        " ~ " => Token::Sub,
    };

    // Starts with '='
    lex_ok! {
        " = ",
        " ~ " => Token::Eq,
    };
    lex_ok! {
        " == ",
        " ~~ " => Token::EqEq,
    };
    lex_ok! {
        " => ",
        " ~~ " => Token::EqArrow,
    };

    // Starts with '%'
    lex_ok! {
        " % ",
        " ~ " => Token::Mod,
    }
    lex_ok! {
        " %{ ",
        " ~~ " => Token::LPatBrace,
    }
    lex_ok! {
        " %[ ",
        " ~~ " => Token::LPatBracket,
    }
    lex_ok! {
        " %( ",
        " ~~ " => Token::LPatParen,
    }
    // Starts with '.'
    lex_ok! {
        " . ",
        " ~ " => Token::Dot,
    };
}

#[test]
fn delimiters() {
    lex_ok! {
                " ( ) { } [ ] ",
                " ~           " => Token::LParen,
                "   ~         " => Token::RParen,
                "     ~       " => Token::LBrace,
                "       ~     " => Token::RBrace,
                "         ~   " => Token::LBracket,
                "           ~ " => Token::RBracket,
    };
}

#[test]
fn string() {
    lex_ok! {
        r#" "\n" "#,
        r#" ~    "# => Token::DQuote,
        r#"  ~~  "# => Token::StringLiteral("\n".into()),
        r#"    ~ "# => Token::DQuote,
    };
    lex_ok! {
        r#" "\r" "#,
        r#" ~    "# => Token::DQuote,
        r#"  ~~  "# => Token::StringLiteral("\r".into()),
        r#"    ~ "# => Token::DQuote,
    };
    lex_ok! {
        r#" "\t" "#,
        r#" ~    "# => Token::DQuote,
        r#"  ~~  "# => Token::StringLiteral("\t".into()),
        r#"    ~ "# => Token::DQuote,
    };
    lex_ok! {
         r#" "\\" "#,
         r#" ~    "# => Token::DQuote,
         r#"  ~~  "# => Token::StringLiteral("\\".into()),
         r#"    ~ "# => Token::DQuote,
    };
    lex_ok! {
        r#" "\"" "#,
        r#" ~    "# => Token::DQuote,
        r#"  ~~  "# => Token::StringLiteral("\"".into()),
        r#"    ~ "# => Token::DQuote,
    };
    lex_ok! {
        r#" "{" "#,
        r#" ~    "# => Token::DQuote,
        r#"  ~  "# => Token::StringLiteral("{".into()),
        r#"   ~ "# => Token::DQuote,
    };
    lex_ok! {
        r#" "}" "#,
        r#" ~    "# => Token::DQuote,
        r#"  ~  "# => Token::StringLiteral("}".into()),
        r#"   ~ "# => Token::DQuote,
    };
    lex_ok! {
        r#" "{}" "#,
        r#" ~    "# => Token::DQuote,
        r#"  ~~  "# => Token::StringLiteral("{}".into()),
        r#"    ~ "# => Token::DQuote,
    };
    lex_ok! {
        r#" "}" "#,
        r#" ~    "# => Token::DQuote,
        r#"  ~   "# => Token::StringLiteral("}".into()),
        r#"   ~  "# => Token::DQuote,
    };

    lex_ok! {
        r#" "a\nb}}" "#,
        r#" ~        "# => Token::DQuote,
        r#"  ~~~~~~  "# => Token::StringLiteral("a\nb}}".into()),
        r#"        ~ "# => Token::DQuote,
    };

    lex_ok! {
        r#" "\"\"" "#,
        r#" ~      "# => Token::DQuote,
        r#"  ~~~~  "# => Token::StringLiteral("\"\"".into()),
        r#"      ~ "# => Token::DQuote,
    };

    lex_ok! {
        r#" "\\\"" "#,
        r#" ~      "# => Token::DQuote,
        r#"  ~~~~  "# => Token::StringLiteral("\\\"".into()),
        r#"      ~ "# => Token::DQuote,
    };

    lex_ok! {
        r#" "\"\"""\"\"" "#,
        r#" ~            "# => Token::DQuote,
        r#"  ~~~~        "# => Token::StringLiteral("\"\"".into()),
        r#"      ~       "# => Token::DQuote,
        r#"       ~      "# => Token::DQuote,
        r#"        ~~~~  "# => Token::StringLiteral("\"\"".into()),
        r#"            ~ "# => Token::DQuote,
    };
    //lex_ko! { r#" "\\\" "#, " ~~~~~ " => ErrorKind::UnterminatedStringLiteral { start: Location::new(1,2,2), end: Location::new(1,7,7) } }
}

#[rustfmt::skip]
#[test]
fn heredoc(){
    lex_ok! {
        r#""""
              """"#,
        r#"~~~"# => Token::HereDocStart,
        r#"~~~~~~~~~~~~~~"# => Token::HereDocLiteral("              ".into()),
        r#"              ~~~"# => Token::HereDocEnd,
    };

}

#[test]
fn test_preprocessor() {
    lex_ok! {
        r#"use "foo.tremor" ;"#,
        r#"~~~               "# => Token::Use,
        r#"    ~             "# => Token::DQuote,
        r#"     ~~~~~~~~~~   "# => Token::StringLiteral("foo.tremor".into()),
        r#"               ~  "# => Token::DQuote,
        r#"                 ~"# => Token::Semi,
    };
}

#[test]
fn test_test_literal_format_bug_regression() {
    let snot = "match %{ test ~= base64|| } of case _ => \"badger\" end ".to_string();

    let mut res = String::new();
    for b in Lexer::new(&snot, arena::Index::default())
        .filter_map(Result::ok)
        .collect::<Vec<TokenSpan>>()
    {
        res.push_str(&format!("{}", b.value));
    }
    assert_eq!(snot, res);
}

#[test]
#[allow(clippy::float_cmp)]
fn lexer_long_float() -> Result<()> {
    let f = 48_354_865_651_623_290_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000.0;
    let source = format!("{:.1}", f); // ensure we keep the .0
    let token = Lexer::new(&source, arena::Index::default())
        .next()
        .ok_or("unexpected")??
        .value;
    assert_matches!(token, Token::FloatLiteral(f_tkn, _) if f_tkn == f);
    Ok(())
}

proptest! {
    // negative floats are constructed in the AST later
    #[allow(clippy::float_cmp)]
#[test]
    fn float_literals_precision(f in 0_f64..f64::MAX) {
        if f.round() != f {
            let float = format!("{:.}", f);
            for token in Lexer::new(&float, arena::Index::default()) {
                assert!(token.is_ok());
            }
        }
    }
}

proptest! {
    // negative floats are constructed in the AST later
    #[allow(clippy::float_cmp)]
    #[test]
    fn float_literals_scientific(f in 0_f64..f64::MAX) {
        let float = format!("{:e}", f);
        for token in Lexer::new(&float, arena::Index::default()).flatten() {
            if let Token::FloatLiteral(f_token, _) = token.value {
                assert_eq!(f, f_token);
            }
        }
    }
}
