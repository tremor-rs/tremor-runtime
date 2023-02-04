# Tremor Demo

This is a docker-compose based demonstration of some of the capabilities of Tremor.

It starts a kafka node, elasticsearch single-node cluster, kibana and influxdb and, last but not least, one Tremor instance.

The two main files to check out are the `demo.yaml` compose file and the `configs/tremor/config/main.troy` file,
containing the plumbing necessary for event routing and the core application logic.

## The demo system

The demo system consists of three Flows.

The first one, called `loadgen`, is constantly pushing data from the `data/data.json.xz` file
in 4 millisecond intervals between single events into a classifying pipeline. This pipeline inspects the structured JSON events for classification and pushes `info` logs into the `info` kafka topic
and `error` logs into the `error` kafka topic. In addition it also applies backpressure, so that, whenever the downstream kafka brokers are overwhelmed, return errors or take longer than 100ms to handle an event, we apply backpressure by throwing away some messages for a short amount of time in order to relieve the downstream a bit.

Another Flow `demo` is consuming the data from the `info` and `error` kafka topics, rate-limits them based on their log classification and batches them up into 50-event batches.
Those batches are finally pushed into the `elastic` cluster into the `tremor` index.

The `metrics` Flow consumes tremor system metrics from pipelines and connectors, enriches them, batches them into batches of 50 events and sends those down to the influx write API (v2) via HTTP.

## Create your own

In order to enjoy the demo system running on your own machine, please follow these steps:

* Make sure you have `docker` and `docker-compose` installed on your machine.
* `cd` into the `tremor-runtime` repo root directory.
* Run `docker-compose -f demo/demo.yaml up`.
* Lean back and watch the docker images being pulled and all the services boot up and enjoy the log message flying by like a flock of birds.
* Inspect the produced data in Kibana and Influxdb.

## Access the produced data in Kibana

* Visit: `http://localhost:5601/app/management/kibana/indexPatterns` and create an index pattern with the pattern `tremor*`.
* Then visit: `http://localhost:5601/app/discover` to see the latest data produced by tremor.

## Access the produced metrics data in influxdb

* Visit: `http://localhost:8086` and signin as user `tremor` with password `snotbadger`.
* In the `data` section, select the bucket `tremor` and create a query to explore the metrics data emitted from tremor.