# Benchmark Framework

> :warning: Those benchmarks are deprecated. Try the ones in `tremor-cli/tests/bench` by running `tremor tests bench tremor-cli/tests`.

The tremor-runtime supports a micro-benchmarking framework via a specialized connector (bench)
Benchmarks output high dynamic range histogram latency reports to
standard output that is compatible with HDR Histogram's plot files [service](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html)

To execute a benchmark, build tremor in **release** mode and run the examples from the tremor repo base directory:

```bash
./bench/run <name>
```

