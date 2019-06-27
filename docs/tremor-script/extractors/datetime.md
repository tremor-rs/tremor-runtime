# Datetime

The datetime extractor parses the input into a timestamp. The format of the target needs to be specified as a parameter to the extractor. The extractor is equivalent to the [Datetime::parse(…)](../functions/datetime/#datetimeparsedatetime-input_format).

## Predicate

When used with `~`, the predicate parses if the target can be parsed to a nanosecond-precision timestamp. The predicate will fail if it encounters any of the errors described in the Error section below.

## Extraction

If the predicate parses, the extractor returns the 64-bit nanosecond-precise UTC UNIX timestamp.

## Example:

```tremor
match { "test" : "2019-01-01 09:42" } of
  case foo = %{ test ~= datetime|%Y-%m-%d %H:%M| } => foo
  default => "ko"
end;
## output: 1546335720000000000
```

## Errors:

The extractor will fail due to one of the following:

* Incorrect input is passed
* Input doesn't match the format passed
* Input doesn't contain the Year, Month, Day, Hour & Minute section irrespective of the format passed.
* Input contains more components than the format passed
