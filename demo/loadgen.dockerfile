FROM tremor-runtime:latest

WORKDIR /root/
COPY demo/loadgen.sh .
COPY demo/data.json.xz .
COPY demo/data.influx.xz .
CMD ["./loadgen.sh"]
