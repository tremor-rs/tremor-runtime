# Profiling Tremor

This is a short canned synopsis of profiling tremor

## Valgrind

We use valgrind, specifically callgrind, to drive call graph optimizations
in tremor.

### Setup on Mac OS X

Install valgrind via homebrew

```bash
$ brew install valgrind
```

### Dependant utilities

The Google Performance Toolkit, gprof2dot, qcachegrind are also required / useful

```bash
$ brew install gprof2dot
$ brew install gperftools
$ brew install qcachegrind
```

### Basic profiling via tremor-cli

This is good enough for initial high-level exploration.

For example, execute a tremor pipeline against recorded data in data.json

```bash
$ valgrind --tool=callgrind target/debug/tremor-cli pipe run tests/configs/ut.combine3-op.yaml data.json
```

Analysing results via google perf toolkit and graphviz for static call flow diagrams

```bash
$ gprof2dot -f callgrind callgrind.out.93972 > pipe.dot
$ dot -Tpng pipe.dot -o pipe.png && open pipe.png
```

Interactive analysis via QCachegrind / KCachegrind

```bash
$ qcachegrind callgrind.out.93972
```

The profiling ( sampling ) frequency is tunable and *SHOULD* be tuned for each run, eg:

```bash
$ RUST_BACKTRACE=1 PROFILEFREQUENCY=1000 valgrind --tool=callgrind \
    target/release/tremor-cli script run examples/config-spike5.tremor data.json
```

Note: When using a **release** build make sure debug symbols are configured in Cargo.toml and enable link time optimisations ( LTO )

## Flamegraphs

Install rust flamegraph support

```bash
$ cargo install flamegraph
```

Perform a benchmark run with flamegraph support ( on Mac OS X )

```bash
$ flamegraph target/release/tremor-server -c bench/real-workflow-througput-json.yaml bench/link.yaml
```

This generates a `flamegraph.svg` file which can be opened in Chrome

```bash
$ open flamegraph.svg
```
