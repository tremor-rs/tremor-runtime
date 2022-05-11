#!/usr/bin/bash
echo "PWD: $PWD"
echo "CWD: $CWD"
export
echo $@
cargo llvm-cov run --no-report -p tremor-cli -- $@