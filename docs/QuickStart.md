

# Quick Start Guide 

This page explains how to get tremor running on a local system for development or testing. There are 2 ways of installing tremor: 

1) Installing tremor on the system without docker
2) Using docker

## Without Docker

### Install Rust

Tremor can be run on any platform without using docker by installing the rust ecosystem. To install the rust ecosystem you can use rustup which is a toolchain installer. 

To install rustup, you run the following command on *nix systems and follow the on-screen instructions:

```bash
curl https://sh.rustup.rs -sSf | sh
```

For Windows (64-bit) you can download the rustup installer by clicking [here](https://win.rustup.rs/x86_64)

Rustup will install all the necessary tools required for rust which includes the compiler and cargo which is the package management tool for rust. 

Tremor is built using the latest stable toolchain, so when asked to select the toolchain during installation, select stable. 

### Running Tremor

After installing rust and cloning the repository from `git.csnzoo.com`, you can start tremor server by changing to `tremor-server` directory in tremor and running: 

```bash
cargo run 
```

To run the test suite, in the root (`tremor-runtime`) directory you can run : 
```bash
cargo test
```

this will run all the tests in the suite, except those which are feature-gated and not needed to quickly test tremor. 

### Additional Rust Tools:

#### Rustfmt:

`Rustfmt` is a tool for formatting rust code according to style guidelines. It maintains consistency in the style in the entire project.

To install `rustfmt` run:

```bash
rustup component add rustfmt
```

To run `rustfmt` on the project, run the following command:

```bash
cargo fmt
```

#### Clippy:

`Clippy` is a linting tool that catches common mistakes and improves the rust code. It is available as a toolchain component and can be installed by running: 

```bash
rustup component add clippy
```

To run `clippy`, run the following comand:

```bash
cargo clippy
```

#### Rustfix:

`Rustfix` automatically applies suggestions made by rustc. There are two ways of using `rustfix` - either by adding it as a library to `Cargo.toml` or by installing it as a cargo subcommand by running:

```bash
cargo install cargo-fix
```

To run it, you can run:

```bash
cargo fix
```

####Tree:

`Cargo tree` is a subcommand that visualizes a crate's dependency-graph and display a tree structure of them. To install it: 

```bash
cargo install cargo-tree
```

To run it: 

```bash
cargo tree
```

#### Flamegraph:

`Flamegraph` is a profiling tool that visualises where time is spent in a program. It generates a SVG image based on the current location of the code and the function that were called to get there. 

To install it: 

```bash
cargo install cargo-flamegraph
```

To run it: 

```bash
cargo flamegraph
```



## With Docker

Tremor contains a dockerfile which makes it easier to run and build using docker. It also contains a makefile so that common docker commands.

To build tremor you can run: 
```bash
make image
```

To run the images:
```bash
make demo
```

Tremor contains integration tests that tests it from a user's perspective. To run the integration tests you can run:
To run the integration tests:
```bash
make it
```

To run clippy,  you can do: 

```bash
make clippy
```
