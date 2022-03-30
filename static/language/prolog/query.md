The `Query` rule defines the logical entry point into Tremor's statement
oriented query language. The query language can be embedded into deployments
via `define pipeline` statements. The query language is used to process
continuous streams of event data via embedded scripts, the state mechanism
and through builtin runtime provided operators, such as the `script` operator
or the `batch` operator or via `select` statements.

Window definitions for use in windowed statements such as those supported by
the `select` statement are also defined and named so that they can be used
by multiple statements and used to compose tilt frames - chains of successive
windows of data in a streaming continuous select query.

The language supports the definition of sub queries through the same `define pipeline`
syntax as in the deployment language.

### Query Language Entrypoint

This is the top level rule of the tremor query language `trickle`

