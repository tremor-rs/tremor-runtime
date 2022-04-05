#!/usr/bin/env bash
# This script sets up the demo for tremor with PDK configured.

export TREMOR_PLUGIN_PATH="etc/tremor/plugins"
export TREMOR_PATH="etc/tremor/config:$TREMOR_PATH"
export RUST_LOG=debug

../target/debug/tremor server run -f etc/tremor/config/config.troy
