# `tremor-script` langauge

## Introduction

This is a developer's introduction to the `tremor-script` language and repo.

## Purpose

To enable a developer with no prior experience of tremor-script to get up and running.

## Scope

Building and running tremor-script from the command line

## Examples ##

[Cheatsheet](./doc/cheatsheet.md).

---

## Fetch source

The `tremor-script` code must be cloned from git

```bash
$ git clone git@git.csnzoo.com:tremor/tremor-runtime
$ cd tremor-runtime/tremor-script
```

## Build library and tremor-script binary

1. Setup [dcargo](https://git.csnzoo.com/tremor/docker-dev-env)
2. Build
  ```bash
  $ dcargo build
  ```
3. Test
  ```bash
  $ dcargo test
  ```

## Native builds ( on OX )

Replace the above `dcargo` commands with `cargo`. We assume latest stable rust is installed via rustup

## Running tremor-script

```bash
$ ./target/debug/tremor-script <file.tremor>
```

If the `TREMOR_SCRIPT_VERBOSE` environment variable is set when running then tokens and astract syntax
tree are printed to standard output.

## Status

Work in progress. Currently lexer, parser and syntax highlighter with error highlighting are done
and the runtime/interpreter is in progress 
