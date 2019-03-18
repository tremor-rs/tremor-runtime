# `tremor-script` language

## Introduction

This is a developer's intro the `tremor-script` language repo.

## Purpose

To enable somebody with no experience to get started with tremor-script.

## Scope

Building tremor-script

## Examples

Look at tremor-script's [cheatsheet](./doc/tremor-script.md) for examples on how to use the language.

---

## Get the repo

The `tremor-script` source code must be cloned onto the OS X file system

```bash
git clone git@git.csnzoo.com:tremor/tremor-script.git
cd tremor-script
```

---

Building on Linux
----------------

Step 1: Follow the instructions setting up `dcargo` here:
https://git.csnzoo.com/tremor/docker-dev-env

Step 2: Building

```shell
dcargo build
```

---

Running tests
-------------
```shell
dcargo test
```

---


Building on OS X
----------------
```shell
cargo build
```

In case of errors try:

Make sure you are using latest rust and have bison >= 3.0.5 installed and on your path

```shell
rustup update stable
export PATH=/usr/local/Cellar/flex/2.6.4/bin/:/usr/local/Cellar/bison/3.0.5/bin:$PATH
cargo clean
cargo build
```


---

Running tests
-------------

```shell
cargo test
```

---
