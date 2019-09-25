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

##### String Interpolation

For strings tremor allows string polation, this means embedding code directly into strings to create strings out of them. This
is a convinience as an alternative of using the string::format function. 
```tremor
"I am a { "string with {1} interpolation." }"
```

##### HereDocs

To deal with pre formatted strings in tremor script we allow for **here docs** they are started by using triple quotes `"""`  that terminate the line (aka `"""bla`  isn't legal) .

Here docs can in indented, the indentation will be truncated to the lowest number of spaces found in any of the lines.

```tremor
"""
    I am
   a
    long
    multi-line
    string
"""
```

The above **heredoc** would truncate 3 spaces characters since `...a` has 3 spaces infant of it even so other lines have 4.

#### Arrays

Array grammar:
> ![](grammar/diagram/Array.png)

Array literals in `tremor-script` are a comma-delimited set of expressions bracketed by the square brakcets '[' and ']'.

```tremor
[ 1, 2, "foobar", 3.456e10, { "some": "json-like-document" }, null ]
```

#### Records

Record grammar:
> ![](grammar/diagram/Record.png)


Field grammar:
> ![](grammar/diagram/Field.png)

Record literals in `tremor-script` are syntactically equivalent to JSON document objects

```tremor
{
  "field1": "value1",
  "field2": [ "value", "value", "value" ],
  "field3": { "field4": "field5" }
}
```

### Paths

Path grammar:
> ![](grammar/diagram/Path.png)

Qualified Segments grammar:
> ![](grammar/diagram/QualifiedSegments.png)

PathSegment grammar:
> ![](grammar/diagram/PathSegment.png)

ArraySegment grammar:
> ![](grammar/diagram/ArraySegment.png)

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

Grab the books from the store (the same using key, index and escaped key notation for field lookup):

```tremor
let capture = event.store.book;
# index and escaped notation can acomodate keys that include 'odd' characters such as whitespaces or dots.
let capture = event.store["book"];
let capture = event.store.`book`;
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

### Const

Const grammer:

![](grammar/diagram/Const.png)

Const can be used to define immutable, constant values that get evaluated at compile time. This is more performant then `let` as all logic can happen at compile time and is helpful for setting up lookup tables or other never changing data structures.

### Let

Let grammar:
> ![](grammar/diagram/Let.png)

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

Drop grammar:
> ![](grammar/diagram/Drop.png)

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

Emit grammar:
> ![](grammar/diagram/Emit.png)

Emit expressions enable short-circuiting the execution of a `tremor-script` when processing
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

There are times when it is necessary to emit synthetic events from `tremor-script` within a
`tremor` `pipeline` to an alternate `operator` port than the default success route. For example,
when data is well-formed but not valid and the data needs to be __diverted__ into an alternate
flow. The emit clause can be deployed for this purpose by specifying an optional named port.

```tremor
emit {
  "event": event,
  "status": "malformed",
  "description":
  "required field `loglevel` is absent"
} => "invalid";
```

### Match

Match grammar:
> ![](grammar/diagram/Match.png)

Match case grammar:
> ![](grammar/diagram/MatchCaseClause.png)

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
  case %[] => "store.book is an array-like data-structure"
  case %{} => "store.book is a record-like data-structure"
  default => "store.book is a primitive data-type"
end
```

Find all fiction books in the store:

```tremor
let found = match store.book of
  case fiction = %[ %{ category ~= "fiction" } ] => fiction
  default => []
end;
emit found;
```

#### Matching literal expressions

> ![](grammar/diagram/ExprCase.png)

The simplest form of case expression in match expressions is matching a literal
value. Values can be any legal tremor-script type and they can be provided as
literals, computed values or path references to local variables, metadata or values
arriving via events.

```tremor
let example = match 12 of
  case 12 => "matched"
  default => drop "not possible"
end;
```

```tremor
let a = "this is a";
let b = " string";
let example = match a + b of
  case "this is a string" => "matched"
  default => drop "not possible"
end;
```

```tremor
let a = [ 1, "this is a string", { "record-field": "field-value" } ];
match a of
  case a => a
  default => drop "not possible"
end;
```

#### Matching on test predicate expressions

It is also possible to perform predicate based matching

```tremor
match "this is not base64 encoded" of
  case ~base64|| => "surprisingly, this is legal base64 data"
  default => drop "as suspected, this is not base64 encoded"
end;
```

These are often referred to informally as `tilde expressions` and tremor supports a
variety of micro-formats that can be used for predicate or test-based matching such
as logstash dissect, json, influx, perl-compatible regular expressions.

Tilde expressions can under certain conditions elementize ( extract ) micro-format
data. The elementization or extraction is covered in the [Extractors](#extractors)
section of this document and in the Extractor reference.

#### Match and extract expressions

It is also possible to elementize or ingest supported micro-formats into
tremor-script for further processing. For example, we can use the `~=` and `~` operator
to perform a predicate test, such as the base64 test in the previous example,
which upon success, extracts ( in the base64 case, decoding ) a value for further
processing.

For example if we had an embedded JSON document in a string, we could test for
the value being well-formed json, and extract the contents to a local variable
as follows:

```tremor
let sneaky_json = "
{ \"snot\": \"badger\" }
";

match sneaky_json of
  case json ~= json|| => json
  default => drop "this is not the json we were looking for"
end;
```

#### Matching array patterns

> ![](grammar/diagram/ArrayCase.png)

Array Pattern grammar:

> ![](grammar/diagram/ArrayPattern.png)

Array Pattern filter grammar:

> ![](grammar/diagram/ArrayPatternFilter.png)

In addition to literal array matching, where the case expression array literal must exactly
match the target of the match expression, array patterns enable testing for matching elements
within an array and filtering on the basis of matched elements.

```tremor
let a = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
match a of
  case %[ 0 ] => "contains zero's"
  default => "does not contain zero's"
end;
```

Predicate matching against supported micro-formats is also supported in array pattern matching.

```tremor
let a = [ "snot", "snot badger", "snot snot", "badger badger", "badger" ];
match a of
  case got = %[ ~re|^(P<hit>snot.*)$| ] => got
  default => "not snotty at all"
end;
```

#### Matching record patterns

> ![](grammar/diagram/RecordCase.png)

Record Pattern grammar

> ![](grammar/diagram/RecordPattern.png)

Record Pattern Fields grammar

> ![](grammar/diagram/RecordPatternFields.png)

Similarly to record literal matching where the case expression record must exactly match the
target of the match expression, record patterns enable testing for matching fields or sub-structures
within a record and extracting and elementizing data on the basis of matched predicate tests ( via `~=` ).

We can check for the presence of fields:

```tremor
match { "superhero": "superman", "human": "clark kent" } of
  case %{ present superhero, present human } => "ok"
  default => "not possible"
end
```

We can check for the absence of fields:

```tremor
match { "superhero": "superman", "human": "clark kent" } of
  case %{ absent superhero, absent human } => "not possible"
  default => "ok"
end
```

We can test the values of fields that are present:

```tremor
match { "superhero": "superman", "human": "clark kent" } of
  case %{ superhero == "superman" } => "we are saved! \o/"
  case %{ superhero != "superman" } => "we may be saved! \o/"
  default => "call 911"
end;
```

We can test for records within records:

```tremor
match { "superhero": { "name": "superman" } } of
  case %{ superhero ~= %{ present name } } => "superman is super"
  case %{ superhero ~= %{ absent name } } => "anonymous superhero is anonymous"
  default => "something bad happened"
end;
```

We can also test for records within arrays within records tersely through nested pattern matching:

```tremor
match { "superhero": [ { "name": "batman" }, { "name": "robin" } ] } of
  case id = %{ superhero ~= %[ %{ name ~= re|^(?P<kind>bat.*)$|} ] } => id
  default => "something bad happened"
end;
```

#### Guard clauses

> ![](grammar/diagram/Guard.png)

Guard expressions in Match case clauses enable matching data structures to be further
filtered based on predicate expressions. For example they can be used to restrict the
match to a subset of matching cases where appropriate.

```tremor
match event of
  case record = %{} when record.log_level == "ERROR" => "error"
  default => "non-error"
end
```

### Merge

> ![](grammar/diagram/Merge.png)

Merge expressions defines a difference against a targetted record and applies that difference to produce
a result record. Merge operations in `tremor-script` follow merge-semantics defined in [RFC 7386](https://tools.ietf.org/html/rfc7386).

```tremor
let event = merge event of {"some": "record"} end
```



|Given|Merge|Result|Explanation|
|---|---|---|---|
|`{"a":"b"}`|`{"a":"c"}`|`{"a":"c"}`|Insert/Update field 'a'|
|`{"a":"b"}`|`{"b":"c"}`|`{"a":"b", "b":"c"}`|Insert field 'b'|
|`{"a":"b"}`|`{"a":null}`|`{}`|Erase field 'a'|
|`{"a":"b","b":"c"}`|`{"a":null}`|`{"b":"c"}`|Erase field 'a'|
|`{"a": [{"b":"c"}]}`|`{"a": [1]}`|`{"a": [1]}`|Replace field 'a' with literal array|


### Patch

> ![](grammar/diagram/Patch.png)

Patch operation grammar

> ![](grammar/diagram/PatchOperation.png)

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
|`let foo = {"foo":"bar"}`|`patch foo of insert "baz" => "qux" end`|`{"foo":"bar","baz":"qux"}`|Add baz field|
|`let foo = {"foo":"bar","baz":"qux"}`|`patch foo of erase "foo" end`|`{"baz":"qux"}`|Erase foo and add baz field|
|`let foo = {"foo":"bar"}`|`patch foo of upsert "foo" => null end`|`{"foo":null}`|Set foo to null, or reset to null if field already exists|

### For comprehensions

> ![](grammar/diagram/For.png)

For Case Clause grammar

> ![](grammar/diagram/ForCaseClause.png)

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

## Extractors

> ![](grammar/diagram/TestExpr.png)

> ![](grammar/diagram/TEST_LITERAL.png)

> ![](grammar/diagram/TEST_ESCAPE.png)

The language has pluggable support for a number of microformats with two basic modes of operation that
enable predicate tests ( does a particular value match the expected micro-format ) and elementization
( if a value does match a specific micro-format, then extract and elementize accordingly.

The general form of a supported micro-format is as follows:

```
<name>|<format>|
```

Where:
* name - The key for the micro-format being used for testing or extraction
* format - An optional multi-line micro-format specific format encoding used for testing and extraction

Formats can be spread out over multiple lines by adding a `\` as a last character of the line. Spaces at the start of the line will be truncated by the lowest number of leading spaces. So if 3 lines respectively have 2, 4, and 7 spaces then 2 spaces are going to be removed from each line leaving 0, 2, and 5 spaces at the start.

The set of supported micro-formats at the time of writing are as follows:

|Name|Format|Test mode|Return type|Extraction mode|
|---|---|---|---|---|
|__base64__|Not required|Tests if underlying value is a base64 encoded string|__string__|Performs a base64 decode, returning a UTF-8 encoded string|
|__glob__|Glob expression|Tests if underlying value conforms to the supplied glob pattern|__string__|Returns the value that matches the glob ( identity extraction )|
|__re__|PCRE regular expression with match groups|Tests if underlying value conforms to supplied PCRE format|__record__|Extracts matched named values into a record|
|__cidr__|Plain IP or netmask|Tests if underlying value conforms to cidr specification|__record__|Extracted numeric ip range, netmask and relevant information as a record|
|__kv__|Logstash KV specification|Tests if the underlying value conforms to Logstash KV specification|__record__|Returns a key/value record|
|__dissect__|Logstash Dissect specification|Tests if the underlying value conforms to Logstash Dissect specification|__record__|Returns a record of matching extractions based on supplied specification|
|__grok__|Logstash Grok specification|Tests if the underlying value conforms to Logstash Grok specification|__record__|Returns a record of matching extractions based on supplied specification|
|__influx__|Not required|Tests if the underlying value conforms to Influx line protocol specification|__record__|Returns an influx line protocol record matching extractions based on supplied specification|
|__json__|Not required|Tests if the underlying value is json encoded|__depends on value__|Returns a hydrated `tremor-script` value upon extraction|

There is no concept of _injector_ in the `tremor-script` language that is analogous to extractors. Where relevant the langauge supports functions that
support the underlying operation ( such as base64 encoding ) and let expressions can be used for assignments.
