# KV

 Parses a string into a map. It is possible to split based on different characters that represent
 either field or key value boundaries.

 A good part of the logstash functionality will be handled ouYeside of this function and in a
 generic way in tremor script.

## Predicate

When used with `~`, the predicate passes if a string is valid that can be converted into a map.

## Extractor

If the predicate passes, it converts the target into its equivalent map.

 Features (in relation to Logstash):

| Setting                |          Supported          |
| :--------------------- | :-------------------------: |
| allow_duplicate_values |             No              |
| default_keys           |     (via tremor script)     |
| exclude_keys           |     (via tremor script)     |
| field_split            | Yes (default `" "` (space)) |
| field_split_pattern    |             No              |
| include_bracke         |     (via tremor script)     |
| include_keys           |     (via tremor script)     |
| prefix                 |     (via tremor script)     |
| recursive              |             No              |
| remove_char_key        |     (via tremor script)     |
| remove_char_value      |     (via tremor script)     |
| source                 |     (via tremor script)     |
| target                 |     (via tremor script)     |
| tag_on_failure         |     (via tremor script)     |
| tag_on_timeout         |             No              |
| timeout_millis         |             No              |
| transform_key          |     (via tremor script)     |
| transform_value        |     (via tremor script)     |
| trim_key               |     (via tremor script)     |
| trim_value             |     (via tremor script)     |
| value_split            | Yes (default `":"` (colon)) |
| value_split_pattern    |             No              |
| whitespace             |     (via tremor script)     |



To specify a value separator (the separator used between key and value) use the pattern form `kv|%{key}=%{val}|`. Both `%{key}` and `%{val}` are fixed keywords and can not be substituted for other names. The pattern `kv|%{key}=%{val}|` would lead to `=` being the separator. Multiple of those pairs can be given to use multiple separators.

To specify field separators they need to be either before or after a value separator or on their own. `kv|&|` would separate the fields by `&`.

Both field and value separators can be related without harm.

Field and value separators can not overlap, even partially.



Complex patterns can be given by using multiple key value pairs with different separators, their order does not matter and they will not be required to be present.



**Example**:

All of the following are equivalent:

```tremor
match { "test" : "foo:bar snot:badger" } of
   case cake = %{ test ~= kv|| } => cake
   case cake = %{ test ~= kv| | } => cake
   case cake = %{ test ~= kv|| } => cake
   case cake = %{ test ~= kv|%{key}:%{val}|} => cake
   case cake = %{ test ~= kv|%{key}:%{val} |} => cake
   case cake = %{ test ~= kv| %{key}:%{val}|} => cake
   case cake = %{ test ~= kv|%{key}:%{val} %{key}:%{val} %{key}:%{val}|} => cake
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

**Example 2**:

Match query parameters

```tremor
match event of
  case cake = %{test ~= kv|%{key}=%{val}&|} => cake
  default => "ko"
end;

  
```

