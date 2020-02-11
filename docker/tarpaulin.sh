#!/usr/bin/env bash

. ~/.cargo/env
export CARGO_TARGET_DIR=target.tarpaulin
cd /code
cargo tarpaulin -v --out Xml --exclude-files target* --exclude-files depricated/* --all -- --test-threads=1
