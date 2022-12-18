## How do i write robust match predicate rules?

There are two basic forms of `match` expression predicate

### Case form

```tremor
  match event of
    case %{} => "I am a possibly non-empty record",
    case %[] => "I am a possibly non-empty array",
    case %( "snot" ) => "I am a list with 1 element which is the string \"snot\"
```

These are used for isolating specific cases of interest that need to specific
processing. Sometimes cases can be incomplete without a `default` case and this
is also supported.

```tremor
    case _ => "If i'm not one of these things, I'm something else"
end
```

If tremor can prove at compile time that a `default` case is advisable it will
emit a warning:

```tremor
match event of
  case true => "I believe you"
end;
```

```tremor
    1 | match event of
    2 |   case true => "I believe you"
    3 | end;
      | ^^^ This match expression has no default clause, if the other clauses do not cover all possibilities this will lead to events being discarded with runtime errors.
```

And, tremor will issue a runtime error when a bad case is found:

```tremor
    1 | match event of
    2 |   case true => "I believe you"
    3 | end;
      | ^^^ A match expression executed but no clause matched
      |     NOTE: Consider adding a `case _ => null` clause at the end of your match or validate full coverage beforehand.

```

It is almost always preferable to have a `default` case and this practice is recommended.

