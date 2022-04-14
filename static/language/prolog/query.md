The `Query` rule defines the logical entry point into Tremor's statement
oriented query grammar. The grammar is embedded into deployments via
`define pipeline` statements.

Pipelines effectively provide a continous streaming abstraction for
event processing. The pipeline query is a graph of connected streams
and operators. The operators can be builtin or provided by users through
the `script` operator.

Window definitions for use in windowed statements such as those supported by
the `select` operation  are also defined and named so that they can be used
by multiple operations and to compose tilt frames - chains of successive
windows of data in a streaming continuous select operation.

The syntax supports the definition of sub queries through the same `define pipeline`
syntax as in the deployment grammar.

