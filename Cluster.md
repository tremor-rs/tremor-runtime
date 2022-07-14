
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


# package
```bash
./target/debug/tremor cluster package --out temp/tick.tar temp/tick.troy 
./target/debug/tremor cluster package --out temp/cs.tar temp/clustered_storage/cs.troy 
```
# install
```bash
./target/debug/tremor cluster apps install temp/cs.tar && \
./target/debug/tremor cluster apps start cs test
```

```bash
./target/debug/tremor cluster apps install temp/tick.tar
./target/debug/tremor cluster apps start tick test
```


websocat ws://0.0.0.0:8080
{"get": "the-key"}

{"put": "the-key", "data": "snot"}
{"put": "the-key", "data": "badger"}

