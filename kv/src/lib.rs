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

// KV parsing
//
// Parses a string into a map. It is possible to split based on different characters that represent
// either field or key value boundaries.
//
// A good part of the logstash functionality will be handled outside of this function and in a
// generic way in tremor script.
//
// Features (in relation to LS):
//
// | Setting                | Translation                                             | Supported |
// |------------------------|---------------------------------------------------------|-----------|
// | allow_duplicate_values | not supported, since we deal with JSON maps             | No        |
// | default_keys           | should be handled in TS (via assignment)                | TS        |
// | exclude_keys           | should behandled in TS (via delete_keys?)               | TS        |
// | field_split            | supported, array of characters                          | Yes       |
// | field_split_pattern    | currently not supported                                 | No        |
// | include_brackets       | should be handled in TS (via map + dissect?)            | TS        |
// | include_keys           | should be handled in TS (via select)                    | TS        |
// | prefix                 | should be handled in TS (via map + string::format)      | TS        |
// | recursive              | currently not supported                                 | No        |
// | remove_char_key        | should be handled in TS (via map + re::replace)         | TS        |
// | remove_char_value      | should be handled in TS (via map + re::replace)         | TS        |
// | source                 | handled in TS at call time                              | TS        |
// | target                 | handled in TS at return time                            | TS        |
// | tag_on_failure         | handled in TS at return time                            | TS        |
// | tag_on_timeout         | currently not supported                                 | No        |
// | timeout_millis         | currently not supported                                 | No        |
// | transform_key          | should be handled in TS (via map + ?)                   | TS        |
// | transform_value        | should be handled in TS (via map + ?)                   | TS        |
// | trim_key               | should be handled in TS (via map + ?)                   | TS        |
// | trim_value             | should be handled in TS (via map + ?)                   | TS        |
// | value_split            | supported, array of characters                          | Yes       |
// | value_split_pattern    | currently not supported                                 | No        |
// | whitespace             | we always run in 'lenient mode' as is the default of LS | No        |

use simd_json::value::borrowed::{Map, Value};

/// Splits a string that represents KV pairs.
///
/// * input - The input string
/// * field_seperator - An array of characters that seperate fields
/// * key_seperator - An array of characters that seperats the key from a value
///
/// Note: Fields that have on value are dropped.
pub fn split<'input>(
    input: &'input str,
    field_seperator: &[char],
    key_seperator: &[char],
) -> Option<Map<'input>> {
    let r: Map = input
        .split(|c| field_seperator.contains(&c))
        .filter_map(|field| {
            let kv: Vec<&str> = field.split(|c| key_seperator.contains(&c)).collect();
            if kv.len() == 2 {
                Some((kv[0].into(), Value::from(kv[1])))
            } else {
                None
            }
        })
        .collect();
    if r.is_empty() {
        None
    } else {
        Some(r)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_split() {
        let r = split("this=is a=test", &[' '], &['=']).expect("Failed to split input");
        assert_eq!(r.len(), 2);
        assert_eq!(r["this"], "is");
        assert_eq!(r["a"], "test");
    }

    #[test]
    fn one_field() {
        let r = split("this=is", &[' '], &['=']).expect("Failed to split input");
        assert_eq!(r.len(), 1);
        assert_eq!(r["this"], "is");
    }

    #[test]
    fn no_split() {
        let r = split("this is a test", &[' '], &['=']);
        assert!(r.is_none());
    }

    #[test]
    fn different_seperatpors() {
        let r = split("this=is;a=test for:seperators", &[' ', ';'], &['=', ':'])
            .expect("Failed to split input");
        assert_eq!(r.len(), 3);
        assert_eq!(r["this"], "is");
        assert_eq!(r["a"], "test");
        assert_eq!(r["for"], "seperators");
    }

}
/*

Functions:

map::select(<map>, [<key>, ...])
  keeps only the given keys in an object this would make include keys be part of TS



*/

/*

https://codesearch.csnzoo.com/source/xref/wayfair%3Apuppet/modules/logstash/templates/conf/logstash.solr.transform.conf.erb

15 kv {
16   source       => "full_message"
17   include_keys => ["hits", "status", "QTime"]
18 }
19
20 kv {
21   source       => "full_message"
22   field_split  => "&"
23   include_keys => ["qt"]
24 }

https://codesearch.csnzoo.com/source/xref/wayfair%3Apuppet/modules/logstash/templates/conf/logstash.solr.transform.conf.erb

92   kv {
93     source       => "naxsi_params"
94     field_split  => "&"
95     include_keys => ["ip", "server", "uri", "learning", "vers", "total_processed", "total_blocked", "block"]
96   }

142       kv {
143         source       => "dispatcher_params"
144         field_split  => "&"
145         include_keys => ["_controller", "_action"]
146         remove_field => "dispatcher_params"
147       }

https://codesearch.csnzoo.com/source/xref/wayfair%3Apuppet/modules/logstash/templates/conf/logstash.git.transform.conf.erb

22     kv {
23       source => "syslog_message"
24       remove_field => [ "syslog_message" ]
25     }

 */
