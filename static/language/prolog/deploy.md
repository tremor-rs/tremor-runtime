The `Deploy` rule defines the logical entry point into Tremor's command
oriented scripting language. The deployment language is the top level
language in tremor's suite of DSLs and defines units of deployment that
the runtime supports.

The language embeds the statement oriented query language and expression
oriented scripting language reusing common rules so that the resulting
system of languages is consistent, shares the same module referencing and
scoping primitives and is idiomatic and relatively easy for tremor systems
programmers to learn.

A legal script is composed of:
* An optional set of module comments
* A sequence of top level expressions. There must be at least one defined.
* An optional end of stream token.

### Deployment Language Entrypoint

This is the top level rule of the tremor deployment language `troy`

