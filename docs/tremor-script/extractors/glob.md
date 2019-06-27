# Glob

Glob is a simple extractor that checks if the input string matches the specified Unix shell style pattern. The extractor fails if an invalid pattern is specified or the string doesn't match the pattern.

## Predicate

When used as a predicate with `~`, the predicate will pass if the input matches the glob pattern passed as the parameter to the extractor.

## Extraction

The extractor returns true if the predicate passes else returns an error

## Patterns

Patterns can be of the following types:

| Pattern | Matches                                                      |
| ------- | ------------------------------------------------------------ |
| `?`     | Single character                                             |
| `*`     | any (0 or more) sequence or characters                       |
| `[…]`   | any character inside the bracket. Supports ranges (e,g. `[0-9]` will match any digit) |
| `[!…]`  | negation of `[…]`                                            |

Meta characters (e..g `*`, `?` ) can be matched by using `[ ]`. (e.g. `[ * ]` will match a string that contains `*`).

```tremor
match { "test" : "INFO" } of
  case foo = %{ test ~= glob|INFO*| } => foo
  default => "ko"
end;
## will output true
```
