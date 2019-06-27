# Extractors

The `tremor-script` language can recognize micro-formats and extract or elementize
data from those micro-formats.

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

In the above example a regular expression `re` extractor is tested ( by use of the `~` test operator ) against the pattern form delimited by `|` symbols. The field level ( that obviously must exist on the event supplied, and requires that event is of record type ) is then tested against the regular expression.

## Extraction Form

The extraction for is similar. In this case the predicate conditions must pass, but the
regular expression can be written with match groups and named matches extracted into a key/value record for further processing.

## Usage

Both the predicate and extraction form of extraction use operators with a `~` ( tilde ) operator. When `~=` the micro-format in use ( a regular expression in our example ) acts as both a predicate test ( is it valid given the test specification ) and an extractor that elementizes and returns a subset of information from the micro-format.

When only validity against the pattern is desired, then the extraction overhead can be eliminated or reduced depending on the implementation of the extractor configured.

## Available extractors

The different extractors available are:

* [Base64](./base64)
* [JSON](./json)
* [Dissect](./dissect)
* [Grok](./grok)
* [Glob](./glob)
* [Cidr](./cidr)
* [KV](./kv)
* [Influx](./influx)
* [Regex (Re)](./regex)
