
### Defines how to interconnect pipeline and connectors

Given

```tremor
create connector ingest;
create connector egress;
create pipeline logic;

connect /connector/ingress/out to /pipeline/logic/in;
connect /pipeline/logic/out to  /connector/egress/in;
```

Defines how the `ingress`, `egress` and `logic` runtime instances
are interconnected for data to flow through them in a specified
order.

