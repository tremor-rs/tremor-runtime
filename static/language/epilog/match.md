
An illustration of the rule is a set of case statements, followed by an optional `default` case

```tremor
  match event of
    case %{} => "I am a possibly non-empty record",
    case %[] => "I am a possibly non-empty array",
    case %( "snot" ) => "I am a list with 1 element which is the string \"snot\"
    case _ => "I'm something else"
  end
```

