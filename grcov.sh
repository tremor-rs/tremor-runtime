#!/usr/bin/env bash

if ! grcov --help > /dev/null
then
  echo "Please install grcov via: cargo install grcov"
  exit 1
fi

CARGO_INCREMENTAL=0
# RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Cinline-threshold=0 -Clink-dead-code -Coverflow-checks=off -Zno-landing-pads"
# no link dead code on os x
RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Cinline-threshold=0 -Coverflow-checks=off -Zno-landing-pads"
CARGO_TARGET_DIR=target.cov
cargo +nightly build --all $CARGO_OPTIONS
cargo +nightly test --all $CARGO_OPTIONS
zip -0 ccov.zip `find . \( -name "tremor*.gc*" -or -name "window*.gc*" -or -name "dissect*.gc*" -or -name "kv*.gc*" \) -print`;
rm -r grcov
mkdir grcov
grcov ccov.zip -s . -t lcov --llvm --branch --ignore-not-existing --ignore-dir "/*" -o grcov/lcov.info;
genhtml --ignore-errors source -o grcov grcov/lcov.info
open grcov/index.html
