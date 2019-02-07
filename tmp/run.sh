#!/bin/bash

set -e
set -u
set -o pipefail

../target/debug/tremor-runtime -m --config config.yaml
