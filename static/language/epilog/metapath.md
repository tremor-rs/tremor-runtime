
### How do i reference event metadata?

Events in tremor encapsulate data from other systems sent and received
from tremor via configured connectors. Information about that data or
metadata can also be provided by the runtime, and used in some operators
and connectors to control tremor's runtime behaviour.

Meta-data is accessed via the `$` dollar symbol.

```tremor
let metadata = $;
```

Metadata can be any legal tremor value, but it is typically a record
structure

```tremor
let metastring = "snot" + $;
```

Meta-data can be written through via a `let` operation

```tremor
let $command = { "do-things": "with-this-meta-request" }
```

