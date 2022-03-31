### How do I define single line string literals?

A single line string MUST be on a single line with raw newline characters ( unless escaped ).

```tremor
"I am a literal string"
```

### How do i define multi line string literals?

A multi line string MUST span multiple lines with raw newline characters.

Multi line strings 
```tremor
"""
I am a
multi
line
string
"""
```

The following example is a malformed multi line string:

```tremor
""" snot """
```

Which when executed will result in a compile time error:

```
Error:
    1 | """ snot """
      | ^^^ It looks like you have characters tailing the here doc opening, it needs to be followed by a newline
```

### Simple and nested Interpolation

Strings in tremor can be interpolated with internal scripts

```tremor
"""

I am an #{interpolated} #{event.sum / event.count} string

Interpolations can be simple, as above, or #{
merge event of
  { "#{snot}": """

    #{badger * 1000 + crazy_snake }

    """ }
}
"""
```

This will result in the output:

```tremor
"\nI am an interpolated 5.0 string\n\nInterpolations can be simple, as above, or {\"sum\":10,\"count\":2,\"snot\":\"\\n    20001\\n\\n    \"}\n"
```

Note that the merge operation merges an event `{ "sum": 10, "count": 10 }` with in scope values of `snot` that evaluates to the literal string `"snot"` and the numerics `badger` ( `20` ) and crazy_snake ( `1` ). Interpolations are nestable and field names or any other string literal in tremor can be interpolated.

However, we do not recommend complex nested interpolated strings. Defining a function and calling it may be a better
alternative for most applications and uses.

