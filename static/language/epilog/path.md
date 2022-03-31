
Path expressions are how events and in memory state in tremor are referenced
in scripts.

### How to reference metadata

The tremor runtime can provide and consume metadata with the events
being processed by the runtime.

Metadata is distinguished by a `$` dollar sign

```tremor
let foo = $request # The local path `foo` is a copy of the metadata `$request`
```

Connectors such as `kafka` and `http` can generate metadata that scripts, queries
and pipelines can manipulate and process to tune tremor's runtime behaviour.

### How to reference the current streaming event

The current event streaming through tremor in the current pipeline will
be available to queries, logics and scripts via the `event` keyword.

The `event` keyword can be further dereferenced via path statements

```tremor
# Where event is a record
let foo = event.snot; # The local path `foo` is a copy of the `snot` field from the current event
```

```tremor
# Where event is an array
let foo = event[10]; The local path `foo` is a copy of the 10th element of the current event.
```

### How to reference pipeline state

Scripts in tremor can store state that is available for the lifetime of a pipeline
via the `state` keyword.

The `state` keyword can be further dereferenced via path statements

```tremor
# Where state is a record
let foo = state.snot; # The local path `foo` is a copy of the `snot` field from the state record 
```

```tremor
# Where state is an array
let foo = state[10]; The local path `foo` is a copy of the 10th element of the state array

### How to reference arguments

For operators and structures that support arguments the `args` keyword can be
used to dereference values via path statements.

```tremor
# Where state is a record
let foo = args.snot; # The local path `foo` is a copy of the `snot` field from the args record 
```

Args are nominal and always record values in tremor.

### How can window state be referenced

Operations supporting windows and groups can dereference the cached state via the
`window` and `group` keywords which both support path operations.

