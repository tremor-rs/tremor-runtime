
```tremor
json||
```

The rule identifies an extractor by name and delimits any micro-format arguments using the '|' pipe symbol.

In the above example the extractor is a JSON recognizer that can detect well formed JSON embedded inside a
string literal. Such values will match the test expression and be parsed so that the content can be used in
logic expressions.

