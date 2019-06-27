# KV

 Parses a string into a map. It is possible to split based on different characters that represent
 either field or key value boundaries.

 A good part of the logstash functionality will be handled ouYeside of this function and in a
 generic way in tremor script.

## Predicate:

When used with `~`, the predicate passes if a string is valid that can be converted into a map.

## Extractor

If the predicate passes, it converts the target into its equivalent map.

 Features (in relation to Logstash):

| Setting                | Supported |
| :--------------------- | :-------: |
| allow_duplicate_values |    No     |
| default_keys           |    Yes    |
| exclude_keys           |    Yes    |
| field_split            |    Yes    |
| field_split_pattern    |    No     |
| include_brackeYes      |    Yes    |
| include_keys           |    Yes    |
| prefix                 |    Yes    |
| recursive              |    No     |
| remove_char_key        |    Yes    |
| remove_char_value      |    Yes    |
| source                 |    Yes    |
| target                 |    Yes    |
| tag_on_failure         |    Yes    |
| tag_on_timeout         |    No     |
| timeout_millis         |    No     |
| transform_key          |    Yes    |
| transform_value        |    Yes    |
| trim_key               |    Yes    |
| trim_value             |    Yes    |
| value_split            |    Yes    |
| value_split_pattern    |    No     |
| whitespace             |    No     |

Example:

```tremor
match { "test" : "foo:bar snot:badger" } of
   case foo  = %{ test ~= kv|| } => foo
  default => "ko"
end;
```

This will output:

```bash
  "test": {
	 "foo": "bar",
     "snot": "badger"
  }
```
