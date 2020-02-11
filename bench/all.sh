#!/bin/bash
./bench/run empty-pipeline-influx
./bench/run empty-pipeline-json
./bench/run empty-pipeline-msgpack
./bench/run empty-pipeline-two-inputs
./bench/run empty-pipeline
./bench/run real-workflow-throughput-json
# ./bench/run real-workflow-throughput-msgpack
./bench/run real-workflow
