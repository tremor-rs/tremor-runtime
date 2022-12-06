
A tuple pattern allows ordinal matching of arrays. A tuple pattern is useful
where the search oriented `%[` .. `]` array pattern syntax is insufficient or
when the order of entries or elements in an array is significant.

```tremor
use std::string;

match string::split(event, "/") of
  case %("snot") => 0	# An array with a single string literal 'snot' value
  case %("snot", ...) => 1 # An array with a first value string literal 'snot', and possibly zero or many more values
  case %("api", _, "badger", ...) => 2 # An array with first value 'api', and 3rd value 'badger'
  case %("") => 3 # An array with an empty string literal value
  case %("badger", "snot") => 4 The two element array with 1st element "badger", and 2nd element "snot"
  case _ => string::split(event, "/")
end
```

