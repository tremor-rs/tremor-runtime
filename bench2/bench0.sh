CFG='[{"class":"default","rate":100000000,"index_key":"wf_index_type"}]'


BLASTER_CONFIG='{"source":"./demo/data.json.xz", "warmup_iters":100, "iters":500, "interval":1}'

BLACKHOLE_CONFIG='{"stop_after_secs": 20, "significant_figures": 1}'

RUST_BACKTRACE=full /usr/bin/time target/release/tremor-runtime-bench --bench-with-metrics-endpoint true --on-ramp blaster --on-ramp-config "${BLASTER_CONFIG}" --parser json --classifier mimir --classifier-config "${CFG}" --grouping bucket --grouping-config "${CFG}" --off-ramp blackhole --off-ramp-config "${BLACKHOLE_CONFIG}" --drop-off-ramp null
