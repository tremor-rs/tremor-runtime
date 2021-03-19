#!/bin/sh

# initial leader
#cargo run -p tremor-cli -- --instance 1 server run --logger-config .backups/logger.yaml --cluster-host "127.0.0.1:8080" --cluster-peer "127.0.0.1:8081" --cluster-peer "127.0.0.1:8082" --cluster-bootstrap
cargo run -p tremor-cli -- --instance 1 server run --logger-config .backups/logger.yaml --cluster-host "127.0.0.1:8180" --network-host "127.0.0.1:8080" --cluster-peer "ws://127.0.0.1:8081" --cluster-bootstrap

# followers
#cargo run -p tremor-cli -- --instance 2 server run --logger-config .backups/logger.yaml --cluster-host "127.0.0.1:8081" --cluster-peer "127.0.0.1:8080" --cluster-peer "127.0.0.1:8082" --api-host "127.0.0.1:9897"
#cargo run -p tremor-cli -- --instance 3 server run --logger-config .backups/logger.yaml --cluster-host "127.0.0.1:8082" --cluster-peer "127.0.0.1:8081" --cluster-peer "127.0.0.1:8082" --api-host "127.0.0.1:9896"
cargo run -p tremor-cli -- --instance 2 server run --logger-config .backups/logger.yaml --cluster-host "127.0.0.1:8181" --network-host "127.0.0.1:8081" --cluster-peer "ws://127.0.0.1:8080" --api-host "127.0.0.1:9897"
#cargo run -p tremor-cli -- --instance 3 server run --logger-config .backups/logger.yaml --cluster-host "127.0.0.1:8182" --network-host "127.0.0.1:8082" --cluster-peer "ws://127.0.0.1:8080" --api-host "127.0.0.1:9897"

# add followers to the cluster
curl -vv -XPOST http://127.0.0.1:9898/cluster/2
curl -vv -XPOST http://127.0.0.1:9898/cluster/3

# ---

# node status checks
curl -s http://localhost:9898/status | jq
curl -s http://localhost:9897/status | jq
curl -s http://localhost:9896/status | jq

# ---

# kv checks
curl http://localhost:9898/kv/snot
curl -s -XPOST http://localhost:9898/kv/snot -d '{"value": "badger"}'
curl http://localhost:9898/kv/snot

# works from followers too
curl http://localhost:9897/kv/snot
curl http://localhost:9896/kv/snot
