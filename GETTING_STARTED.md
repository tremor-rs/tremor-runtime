# Getting Started with Tremor

A guide to help you get Tremor up and running on your machine.

## Prerequisites

You should have Docker and Git installed and configured. You will need to have access to the engineering vlan VPN.

## Repository Set-Up

You will need to open a terminal, and clone the repo. This repo has the Dockerfile, which builds the image for Tremor.

To do this, type in the shell:

```sh
git clone git@git.csnzoo.com:data-engineering/tremor-runtime
```

## Tremor Docker Image

You will need to be in the tremor-runtime directory to make a Docker image.

To change to tremor-runtime directory, type in the shell:

```sh
cd tremor-runtime
```

Now that you are in the tremor-runtime directory enter:

```sh
make tremor-image
```

This is creating the local Docker image `tremor-runtime`.

## Container Configuration

This Docker image together with a `.yaml` file, which contains the configuration, will make the Docker container.
To create the `.yaml`, you should use the `README.md` as a guide and set to your configuration.

To help you with your configuration, here is an `example.yaml`, which you can use a template. You can fill in or delete to your specifications. On-ramp and Off-ramp are required to be specified. Other values may be left out, and the default value as documented in the README is used. Steps without a configuration do not need to specify the `_CONFIG`.

```yaml
version: '3.3'
services:
  tremor:
    image: tremor-runtime
    ports:
    # Port where we export metrics from
      - 9898:9898
    environment:
    # Source of events, the list of possible on-ramps and their configurations are in the README.md under the section On-ramp
      - ONRAMP=kafka
      - 'ONRAMP_CONFIG={"group_id":"demo","topics":["MSSQL_META_DATA_RAW"],"brokers":["distsysberlinc1n1.dev.bo1.csnzoo.com:9092", "distsysberlinc1n2.dev.bo1.csnzoo.com:9092", "distsysberlinc1n3.dev.bo1.csnzoo.com:9092"]}'
    # Destination for events, the list of possible off-ramps and their configuration is in the README.md under the section Off-ramps
      - OFFRAMP=es
      - 'OFFRAMP_CONFIG={"endpoints":["http://elastichotc3n1.dev.bo1.csnzoo.com:9092", "http://elastichotc3n2.dev.bo1.csnzoo.com:9092", "http://elastichotc3n3.dev.bo1.csnzoo.com:9092"], "index":"metadata","batch_size":100,"batch_timeout":500}'
    # Diverted events are sent here, same configurations are possible as in off-ramps
      - DIVERTED_OFFRAMP=null
      - DIVERTED_OFFRAMP_CONFIG=
    # The parser tells tremor the format the events have, and the list of possible parsers and their configuration is in the README.md under the section Parser
      - PARSER=json
      - PARSER_CONFIG=  
    # The classifier handles event classification, the list of possible classifiers and their configuration is in the README.md under the section Classifier
      - CLASSIFIER=mimir
      - CLASSIFIER_CONFIG=[]
    # The grouping defines how events are grouped, the list of possible groupers and their configuration is in the README.md under the section Grouping
      - GROUPING=pass
      - GROUPING_CONFIG=
    # The limiting is performed after the grouping, based on back pressure, the list of possible limiters and their configuration is in the README.md under the section Limiting
      - LIMITING=pass
      - LIMITING_CONFIG=
```

After completing the `exmaple.yaml`, you need to start the container, type in the shell:

```sh
docker-compose -f path/to/your/example.yaml up
```