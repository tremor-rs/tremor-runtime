# Tremor Workshop
[Tremor Workshop]: #tremor-workshop

This workshop serves as a getting started laboratory for downloading,
compiling and running tremor on development machines and developing
tremor-based solutions.


## Table of Contents
[Table of Contents]: #table-of-contents

  - [Development Environment](#tremor-dev-env)

## Download and environment setup
[Tremor Download and Setup]: #tremor-dev-env

### Download and build quick start

Make sure your environment is setup for building tremor:

[Tremor Quick Start](https://github.com/wayfair-incubator/tremor-runtime/blob/master/docs/development/quick-start.md)

### Setup support for your IDE

Tremor supports language server extensions for VS Code and VIM text editors/IDEs:

[Tremor Language Server](https://github.com/wayfair-incubator/tremor-language-server)


### Make sure binaries are on your PATH

```bash
$ export PATH=/Path/to/tremor-src-repo/target/debug/:$PATH
$ tremor-query --version
tremor-query 0.6.0
```
