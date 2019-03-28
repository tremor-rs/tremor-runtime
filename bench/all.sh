#!/bin/bash
./bench/run empty-pipeline-influx
./bench/run empty-pipeline-json
./bench/run empty-pipeline-msgpack
./bench/run empty-pipeline-two-inputs
./bench/run empty-pipeline
./bench/run real-workflow-througput-json
./bench/run real-workflow-througput-msgpack
./bench/run real-workflow
