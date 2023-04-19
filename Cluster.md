
# Initialize
```bash
rm -r temp/test-db*; cargo run -p tremor-cli -- cluster bootstrap --db-dir temp/test-db1 --api 127.0.0.1:8001 --rpc 127.0.0.1:9001
```

# Join
```
target/debug/tremor cluster start --db-dir temp/test-db2 --api 127.0.0.1:8002 --rpc 127.0.0.1:9002 --join 127.0.0.1:8001

target/debug/tremor cluster start --db-dir temp/test-db3 --api 127.0.0.1:8003 --rpc 127.0.0.1:9003 --join 127.0.0.1:8002
```

# Restart
```
target/debug/tremor cluster start --db-dir temp/test-db2 --api 127.0.0.1:8002 --rpc 127.0.0.1:9002
target/debug/tremor cluster start --db-dir temp/test-db3 --api 127.0.0.1:8003 --rpc 127.0.0.1:9003
```

./target/debug/tremor cluster package --out temp/cs.tar temp/clustered_storage/cs.troy && \
./target/debug/tremor cluster apps install temp/cs.tar && \
./target/debug/tremor cluster apps start cs test

curl -v -X POST -H 'Content-Type: application/json' localhost:8001/v1/api/kv/write -d '{"key": "the-key", "value": "\"snot\""}'
