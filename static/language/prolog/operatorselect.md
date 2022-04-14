The `OperatorSelect` rule provides the `select` statement in streaming queries.

The statement has
* A target expression
* A stream and port from which to consume events
* A stream and port to which synthetic events are produced
* An optional set of iwndow definitions
* An optional `where` filter
* An optional `having` filter
* An optional `group by`

Unlike ANSI-ISO SQL select operations in tremor do not presume tabular or columnar data. The
target expression can be any well-formed and legal value supported by tremor.

