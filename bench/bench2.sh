#!/bin/sh

. $(dirname $0)/common.inc

CFG='[{"class":"default","rate":0,"index_key":"wf_index_type"}]'


BLASTER_CONFIG='{"source":"./demo/data.json.xz", "warmup_iters":100, "iters":1000000000, "interval":0}'

BLACKHOLE_CONFIG='{"stop_after_secs": 120, "significant_figures": 2}'

reshlt=$(target/release/examples/bench --no-metrics-endpoint --on-ramp blaster --on-ramp-config "${BLASTER_CONFIG}" --parser json --classifier mimir --classifier-config "${CFG}" --grouping bucket --grouping-config "${CFG}" --off-ramp null --drop-off-ramp-config "${BLACKHOLE_CONFIG}" --drop-off-ramp blackhole)

publish "$result"
