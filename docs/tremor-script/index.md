# Tremor-Script

The `tremor-script` scripting language is an interpreted expression-oriented language
designed for the filtering, extraction, transformation and streaming of structured
data in a stream or event-based processing system.

At its core, `tremor-script` supports a structured type system equivalent to JSON. It
supports integer, floating point, boolean and UTF-8 encoded string literals, literal
arrays and associative dictionaries or record types in addition to a `null` marker.

A well-formed JSON document is a legal tremor-script expression.

## Principles

### Safety

The language is explicitly not Turing-complete:
 - there are no unstructured `goto` grammar forms
 - there are no unbounded `for`, `while` or `do..while` looping constructs
 - the language is built on top of rust, inheriting its robustness and safety features, without the development overheads

### Developer friendly

The language adopts a fortran-like syntax for key expression forms and has
a path-like syntax for indexing into records and arrays

### Stream-oriented / event-based

Tremor-script is designed to process unstructured ( but well-formed ) data events.
Event data can be JSON, MsgPack or any other form supported by the tremor event
processing system.

### Self-documenting

The fortran-like syntax allows rich operations to be computed against arbitrary JSON-like
data. For example JSON documents can be `patch`ed and `merge`ed with operation and document
based templates. Records and Arrays can be iterated over to transform them, merge them with
other documents or to extract subsets for further processing

### Extensibility

The expression-based nature of `tremor-script` means that computational forms and any processing
is transient. The language describes a set of rules ( expressions ) that process an inbound event document
into an outbound documented emitted after evaluation of the script.

The core expression language is designed for reuse in other script-based DSLs and can currently be extended
through its modular function subsystem.

The language also supports a pluggable data extraction model allowing base64 encoded, regular expressions and
other micro-formats encoded within record fields to be referenced based on their internal structure and for
subsets to be mapped accordingly.

In the future, `tremor-script` may be retargeted as a JIT-compiled language.

### Performant

Data ingested into tremor-script is vectorized via SIMD-parallel instructions on x86-64 or other Intel processor
architectures supporting ssev3/avx extensions. Processing streams of such event-data incurs some allocation overhead
at this time, but these event-bound allocations are being written out of the interpreter.

The current meaning of `performant` as documented here means that `tremor-script` is more efficient at processing
log-like data than the system it replaces ( logstash - which mixes extraction plugins such as `grok` and `dissect`
with JRuby scripts and a terse configuration format )

### Productive

The `tremor-script` parsing tool-chain has been designed with ease-of-debugging and ease-of-development in mind. It
has buitin support for syntax-highlighting on the console with errors annotating highlighted sections of badly written
scripts to simplify fixing such scripts.

## Language

This section details the major components of the `tremor-script` language

### Comments

Comments in `tremor-script` are single-line comments that begin with a '#' symbol and continue until end of line.

```tremor
# I am a comment
```

### Literals

Literal in `tremor-script` are equivalent to their sibling types supported by the JSON format.

#### Null

The null literal which represents the abscence of a defined value

```tremor
null
```

#### Boolean

Boolean literal.

```tremor
true
```

```tremor
false
```

#### Integer Numerics

Integers in `tremor-script` are signed and are limited to 64-bit internal representation

```tremor
404
```

#### Floating-Point Numerics

Floating point numerics in `tremor-script` are signed and are limited to 64-bit IEEE representation

```tremor
1.67-e10
```

#### Character and Unicode Code-points

The language does not support literal character or Unicode code-points at this time.

#### UTF-8 encoded Strings

```tremor
"I am a string"
```

String literals in `tremor-script` can span multiple lines

```tremor
" I
am 
a 
long
multi-line
string
"
```

#### Arrays

Array literals in `tremor-script` are a comma-delimited set of expressions bracketed by the square brakcets '[' and ']'.

```tremor
[ 1, 2, "foobar", 3.456e10, let a = 10, { "some": "json-like-document" }, null ]
```

#### Records

Record literals in `tremor-script` are syntactically equivalent to JSON document objects

```tremor
{
  "field1": "value1",
  "field2": [ "value", "value", "value" ],
  "field3": { "field4": "field5" }
}
```

### Paths

Path-like structures in `tremor-script` allow a subset of an ingested event, meta-data passed to the tremor-script function
and script-local data to be indexed. For example the following document

Example event for illustration purposes:

```json
{
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      {
        "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
      },
      {
        "category": "fiction",
        "author": "J.R.R. Tolkien",
        "title": "The Lord of the Rings",
        "isbn": "0-395-19395-8",
        "price": 22.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  },
  "expensive": 10
}
```

Grab the entire event document:

```
let capture = event;
```

Grab the books from the store:

```tremor
let capture = event.store.book;
```

Grab the first book:

```tremor
let capture = event.store.book[0];
```

Grab the title of the 3rd book:

```tremor
let capture = event.store.book[2].title
```

Grab the range of books from 0 ( the first ) to 2 ( the last ), exclusive of the last book:

```tremor
let capture = event.store.book[0:2];
```

The type of a path is equivalent to the type of the data returned by a path expression. So in
the above examples, a reference to a book title would return the value at that path, which in
the reference event document is a `string`.

Path's in `tremor-script` are themselves expressions in their own right.

### Let

The let expression allows data pointed to by a path to be destructively mutated, and the pointed-to
value reassigned. If the path does not yet exist, it will be created in-situ:

Set a local variable `a` to the literal integer value 10:

```tremor
let a = 10;
```

Set a local variable `a` to be the ingested event record

```tremor
let a = event;
```

Set the global variable `a` to be the value of the local variable `a`:

```tremor
let $a = a;
```

### Drop

Drop expressions enable short-circuiting the execution of a `tremor-script` when badly formed
data is discovered. If no argument is supplied, `drop` will return the event record. If an
argument is supplied, the result of evaluating the expression will be returned. Tremor or
other processing tools can process dropped events or data using purpose-built error-handling.

As the content of the dropped event is user-defined, operators can standardise the format of
the error emitted on drop from `tremor-script`

```tremor
drop "oh noes!";
drop "never happens"; # As the first emit always wins, this expression never executes
```

### Emit

Emit expressions enable short-circuiting the execution of a `tremor-script when processing
is known to be complete and further processing can be avoided. If no argument is supplied,
`emit` will return the event record. If an argument is supplied, the result of evaluating the expression
will be returned. Tremor or other processing tools can process emitted events or data using their
default flow-based or stream-based data processing pipelines.

As the content of the emitted event is user-defined, oeprators can standardise the format
of the event emitted on emit from `tremor-script`

*NOTE* By default, if no `emit` or `drop` expressions are defined, all expressions in a correctly
written tremor-script will be executed until completion and the value of the last expression evaluated
will be returned as an `emit` message.

Implicit emission:

```tremor
"badgers" # implicit emit
```

Explicit emission of `"snot"`:

```tremor
"badgers" # literals do not short-circuit processing, so we continue to the next expression in this case
emit "snot"
```

```tremor
emit "oh noes!"
emit "never happens"; # As the first emit always wins, this expression never executes
```

### Match

Match expressions enable data to be filtered or queried using case-based reasoning. Match expressions
take the form:

```tremor
match <target> of
case <case-expr> [ <guard> ] => <block>
...
default => <block>
end
```

Where:
- target: An expression that is the target of case-based queries
- case-expr: A predicate test, literal  value or pattern to match against
- guard: An optional predicate expression to gate whether or not an otherwise matching case-clause will in fact match
- block: The expression to be evaluated if the case matches, and any supplied guard evaluates to true

Examples:

Discover if the `store.book` path is an array, record or primitive structure:

```tremor
match store.book of
  case %[] => "store.book is an array-like data-structure",
  case %{} => "store.book is a record-like data-structure",
  default => "store.book is a primitive data-type"
end
```

Find all fiction books in the store:

```tremor
let found = match store.book of
  case fiction = %[ %{ category ~= "fiction" } ] => fiction,
  default => []
end;
emit found;
```

### Merge

Merge expressions defines a difference against a targetted record and applies that difference to produce
a result record. Merge operations in `tremor-script` follow merge-semantics defined in [RFC 7386](https://tools.ietf.org/html/rfc7386).

|Given|Merge|Result|Explanation|
|---|---|---|---|
|`{"a":"b"}`|`{"a":"c"}`|`{"a":"c"}`|Insert/Update field 'a'|
|`{"a":"b"}`|`{"b":"c"}`|`{"a":"b", "b":"c"}|Insert field 'b'|
|`{"a":"b"}`|`{"a":null}`|`{}`|Erase field 'a'|
|`{"a":"b","b":"c"}`|`{"a":null}`|`{"b":"c"}`|Erase field 'a'|
|`{"a": [{"b":"c"}]}`|`{"a": [1]}`|`{"a": [1]}`|Replace field 'a' with literal array|


### Patch

Patch expressions define a set of record level field operations to be applied to a target record in order
to transform a targetted record. Patch allows fields to be: inserted where there was no field before; removed
where there was a field before; updated where there was a field before; or inserted or updated regardless of
whether or not there was a field before. Patch also allows field level merge operations on records or for the
targetted document itself to be merged. Merge operations in patch are syntax sugar in that they are both based
on the merge operation.

Patch follows the semantics of [RFC 6902](https://tools.ietf.org/html/rfc6902) with the explicit exclusion of
the `copy` and `move` operations and with the addition of an `upsert` operation the variant supported by `tremor-script`

|Example|Expression|Result|Explanation|
|---|---|---|---|
|`let foo = {"foo":"bar"}`|`patch foo of insert baz => "qux" end`|`{"foo":"bar","baz":"qux"}`|Add baz field|
|`let foo = {"foo":"bar"}`|`patch foo of erase foo, baz => "qux" end`|`{"baz":"qux"}`|Erase foo and add baz field|
|`let foo = {"foo":"bar"}`|`patch foo of upsert bar => null end`|`{"foo":null}`|Set foo to null, or reset to null if field already exists|

### For comprehensions

For expressions are case-based record or array comprehensions that can iterate over index/element or key/value
pairs in record or array literals respectively.

Given our book store example from above:

```tremor
let wishlist = for store.book of
  case (i,e) =>
    for e of of
      case (k,v) when k == "price" and v > 20.00 => { "title": e.title, "isbn": e.isbn }
      default => {}
    end
end
```

### Advanced

To be done

#### Patch vs Merge

#### Performance tuning
