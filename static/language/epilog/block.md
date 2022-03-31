### How does tremor process blocks?

A block of expressions is a `;` semi-colon delimited set of statements that
share the same scope. This means that the same set of metadata, `state` and
any scoped identifiers are visible to the block.

The last expression in a block of statements is the return value.

```tremor
let return = 1;
let return = return << 7 % 4;
return - 1
```

The answer as we're sure you'll agree is `7`.

