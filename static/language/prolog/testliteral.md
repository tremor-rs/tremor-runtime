The `TestLiteral` rule specifies an extractor microformat block.

An extractor takes the general form:

```ebnf
Ident '|' MicroFormat '|'
```

Where

The `ident` is the name of a builtin extractor such as `json` or `base64`.

The `Microformat` content depends on the extractor being used

