FROM tremor-runtime:latest

WORKDIR /root/
COPY demo/loadgen.sh .
COPY demo/data.json.xz .
CMD ["./loadgen.sh"]
