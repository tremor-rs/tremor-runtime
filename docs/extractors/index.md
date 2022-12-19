# Extractors

The `tremor-script` language can recognize micro-formats and extract or elementize data from those micro-formats.

There are two basic variants of this in the language:

## Predicate Form

In the predicate form the `~` ( tilde ) operator performs a test to see if the
associated micro-format pattern matches a supplied value.

```tremor
match event of
  case %{ level ~ re|^ERR.*$| } => "is an error"
  default => "is not an error"
end
```

In the above example a regular expression `re` extractor is tested ( by use of the `~` test operator ) against the pattern form delimited by `|` symbols. The field level ( that must exist on the event supplied, and requires that event is of record type ) is then tested against the regular expression.

## Extraction Form

The extraction for is similar. In this case the predicate conditions must pass, but the regular expression can be written with match groups and named matches extracted into a key/value record for further processing.

## Usage

Both the predicate and extraction form of extraction use operators with a `~` ( tilde ) operator. When `~=` the micro-format in use ( a regular expression in our example ) acts as both a predicate test ( is it valid given the test specification ) and an extractor that elementizes and returns a subset of information from the micro-format.

When only validity against the pattern is desired, then the extraction overhead can be eliminated or reduced depending on the implementation of the extractor configured.

Extractors implicitly check if a field is present. If a field isn't present in the record, the predicate will fail and it will check the next predicate. Thus, no further explicit check such as `present <field>` is required.

## Note

Extractors should not be followed by a function that uses the same field as the one in the extractor. This could result in unintended behaviour as the latter function might return the original value instead of the extracted value

e.g.

```tremor
match { "superman" = "message_key: ruler: batman" } of
  case r = %{superman ~= grok|(?<message_key>(.\|\\n){0,200})|, present superman}
    => let event.new_ruler = r.superman.name
 default => "switch to marvel"
end;
```

This will result in unintended behaviour because `present` will return the original string instead of the record extracted by gr