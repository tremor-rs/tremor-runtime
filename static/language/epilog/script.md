### Type system

Tremor supports a data oriented or value based type system with a syntax
that is backwards compatible with JSON.

Any well-formed and legal JSON document is a valid literal in tremor.

Tremor literals for `null`, `boolean`, `string` ( utf-8 ), integer ( 64-bit unsigned ),
 float ( 64-bit ieee ), arrays, and records are equivalent to their JSON counterparts.

Tremor also supports a binary literal for transporting and processing opaque binary data.

### Asymmetric

JSON literals are valid tremor value literals.

Tremor literals MAY NOT always be valid JSON literal.


```tremor
# The following literal is valid JSON and valid Tremor
[1, "snot", {}];

# The following literal is valid in tremor only
[1, "snot", {}, << data/binary >>, ];
```

Tremor supports comments, JSON does not.
Tremor supports trailing commas in arrays and records, JSON does not.
Tremor supports binary literal data, JSON does not.

Note: By default, most connectors in tremor serialize to and from `json` via a codec. The
type system in tremor however is agnostic to the wire format of data that flows through
tremor. So data originate as `json`, as `msgpack`.

### Computations

Tremor also supports a rich expression language with the same support for additive, mutliplicate,
comparitive, and logical unary and binary expressions as languages like `rust` and `java`.

As most of the data that flows through tremor is heirarchically structured or JSON-like tremor
also has rich primitives for structural pattern matching, structural comprehension or iterating
over data structures.

### Loops

Tremor does not support `while` `loop` or other primitives that can loop, recurse or iterate
indefinitely.

In an event based system, events are streaming continuously - so infinite loops that can block
streams from making forward progress are considered harmful.

There are no loops.

We do support iteration over finite arrays.

We do support depth-limited tail recursive functional programming.

### Expression oriented

The script processing is expression oriented. This is to say that every structural
form supported by tremor returns a data structure as a result.


### Event oriented

Scripts in tremor can `emit` or `drop` an `event that is being processed.

The `event` keyword is the subject. It identifies the value currently being processed.

The `emit` keyword halts processing succesfully with a value.

The `drop` keyword halts processing by discarding the current event.


### Illustrative example

```tremor
# Propagate events marked as important and convert them to system alerts
match event of
  case %{ present important } => { "alert": event.message }
  case _ => drop
end;
```

