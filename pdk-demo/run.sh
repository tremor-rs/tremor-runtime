#!/usr/bin/env bash
# This script sets up the demo for tremor with PDK configured.

export TREMOR_PLUGINS_PATH="plugins"
export TREMOR_PATH="etc/tremor/config:$TREMOR_PATH"
export RUST_LOG=info

../target/debug/tremor server run -f etc/tremor/config/config.yaml etc/tremor/config/main.trickle
