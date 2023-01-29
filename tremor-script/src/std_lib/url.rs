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

use crate::registry::Registry;
use crate::tremor_fn;
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};
use url::Url;

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_fn! (url|decode(ctx, s: String) {
            let ds = percent_decode_str(s).decode_utf8();
            if let Ok(decoded) = ds {
                Ok(Value::from(decoded.to_string()))
            } else {
                Err(to_runtime_error(format!("Could not urldecode value: {s}")))
            }
        }))
        .insert(tremor_fn!(url|construct(ctx, scheme: String, host: String, path: String, query: String, fragment: String) {
            let url = Url::build(|b| {
                b.scheme(scheme)
                 .host(host)
                 .path(path)
                 .query(query)
                 .fragment(fragment)
            });
            match url {
                Ok(url) => Ok(Value::from(url.as_str().to_owned())),
                Err(e) => Err(to_runtime_error(format!("Error building URL: {}", e))),
            }
        }))
        .insert(tremor_fn!(url|set_scheme(ctx, url: String, scheme: String) {
            let parsed_url = Url::parse(&url);
            match parsed_url {
                Ok(mut parsed_url) => {
                    parsed_url.set_scheme(scheme).unwrap();
                    Ok(Value::from(parsed_url.as_str().to_owned()))
                }
                Err(e) => Err(to_runtime_error(format!("Error parsing URL: {}", e))),
            }
        }))
        .insert(tremor_fn!(url|set_host(ctx, url: String, host: String) {
            let parsed_url = Url::parse(&url);
            match parsed_url {
                Ok(mut parsed_url) => {
                    parsed_url.set_host(Some(host)).unwrap();
                    Ok(Value::from(parsed_url.as_str().to_owned()))
                }
                Err(e) => Err(to_runtime_error(format!("Error parsing URL: {}", e))),
            }
        }))
        .insert(tremor_fn!(url|set_path(ctx, url: String, path: String) {
            let parsed_url = Url::parse(&url);
            match parsed_url {
                Ok(mut parsed_url) => {
                    parsed_url.set_path(path);
                    Ok(Value::from(parsed_url.as_str().to_owned()))
                }
                Err(e) => Err(to_runtime_error(format!("Error parsing URL: {}", e))),
            }
        }))
        .insert(tremor_fn!(url|set_query(ctx, url: String, query: String) {
            let parsed_url = Url::parse(&url);
            match parsed_url {
                Ok(mut parsed_url) => {
                    parsed_url.set_query(Some(query));
                    Ok(Value::from(parsed_url.as_str().to_owned()))
                }
                Err(e) => Err(to_runtime_error(format!("Error parsing URL: {}", e))),
            }
        }))
        .insert(tremor_fn! (url|set_fragment(url: String, fragment: String) {
            let parsed_url = Url::parse(&url);
            match parsed_url {
                Ok(mut parsed_url) => {
                    parsed_url.set_fragment(Some(&fragment));
                    Ok(Value::from(parsed_url.into_string()))
                }
                Err(e) => Err(to_runtime_error(format!("Error parsing URL: {}", e)))
            }
        }))
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use crate::Value;

    macro_rules! assert_val {
        ($e:expr, $r:expr) => {
            assert_eq!($e, Ok(Value::from($r)))
        };
    }

    #[test]
    fn shook_endecode_smoke_test() {
        let d = fun("url", "decode");
        let e = fun("url", "encode");

        let v = Value::from("snot badger");
        assert_val!(e(&[&v]), "snot%20badger");

        let v = Value::from("%22snot%20badger%22");
        assert_val!(d(&[&v]), r#""snot badger""#);
    }

    #[test]
    fn url_construct_smoke_test() {
    let c = fun("url", "construct");
    assert_val!(c(&[Value::from("http"), Value::from("example.com"), Value::from("/path"), Value::from("query"), Value::from("fragment")]), "http://example.com/path?query#fragment");
    }

    #[test]
    fn url_set_scheme_smoke_test() {
    let c = fun("url", "set_scheme");
    assert_val!(c(&[Value::from("http://example.com"), Value::from("https")]), "https://example.com");
    }

    #[test]
    fn url_set_host_smoke_test() {
    let c = fun("url", "set_host");
    assert_val!(c(&[Value::from("http://example.com"), Value::from("newexample.com")]), "http://newexample.com");
    }

    #[test]
    fn url_set_path_smoke_test() {
    let c = fun("url", "set_path");
    assert_val!(c(&[Value::from("http://example.com"), Value::from("/newpath")]), "http://example.com/newpath");
    }

    #[test]
    fn url_set_query_smoke_test() {
    let c = fun("url", "set_query");
    assert_val!(c(&[Value::from("http://example.com"), Value::from("newquery")]), "http://example.com?newquery");
    }

    #[test]
    fn url_set_fragment_smoke_test() {
    let c = fun("url", "set_fragment");
    assert_val!(c(&[Value::from("http://example.com"), Value::from("newfragment")]), "http://example.com#newfragment");
    }

}