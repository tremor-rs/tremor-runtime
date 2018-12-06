# `mimir` language

## Introduction

This is a developer's intro the `mimir` language repo.

## Purpose

To enable somebody with no experience to get started with mimir.

## Scope

Building mimir

## Examples

Look at mimir's [cheatsheet](./doc/mimir.md) for examples on how to use the language.

---

## Get the repo

The `mimir` source code must be cloned onto the OS X file system

```bash
git clone git@git.csnzoo.com:data-engineering/mimir.git
cd mimir
```

---

Building on Linux
----------------

Step 1: Follow the instructions setting up `dcargo` here:
https://git.csnzoo.com/data-engineering/docker-dev-env

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
WARNING: unsupported

```shell
cargo build
```

---

Running tests
-------------

```shell
cargo test
```

---