Imports definitions from an external source for use in the current source file.

The contents of a source file form a module.

Multime modules that share a common root can be combined into a single use clause by using `use some::root::{m1, m2::sub_module::{m3, m4}}`

### TREMOR_PATH

The `TREMOR_PATH` environment path variable is a `:` delimited set of paths.

Each path is an absolute or relative path to a directory.

When using relative paths - these are relative to the working directory where the
`tremor` executable is executed from.

The tremor standard library MUST be added to the path to be accessible to scripts.

