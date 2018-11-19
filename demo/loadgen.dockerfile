FROM tremor-runtime:latest

COPY demo/loadgen.sh .
COPY demo/data.json.xz .
COPY demo/data.influx.xz .
ENTRYPOINT ["./loadgen.sh"]
