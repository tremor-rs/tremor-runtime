This tool allows to configure a pipeline that moves data from a source to a destination. The pipeline consists of multiple steps which are executed in order. Each step is configurable on a plugin basis.


# Externals

[Jenkins](https://containers.jenkins.service.bo1.csnzoo.com/blue/organizations/jenkins/build-docker-data-engineering-tremor-runtime/activity)

[Artifactory](https://artifactory.service.bo1.csnzoo.com/artifactory/webapp/#/artifacts/browse/tree/General/docker-wayfair/wayfair/data-engineering/tremor-runtime)

# Plugins
Some plugins require additional configuration to be passed others do not.

## Input
The input step defines the source of the data.

### stdin
The `stdin` plugin reads from STDIN each line is treated as one message.

### kafka
The `kafka` plugin reads from a given kafka topic. The configuration passed is: `<group-id>|<topic>|<broker list>`. Messages are only comitted as processed when the pipeline finished.

## Parser
Parsers handle converting the message from the raw binary to a representation format.

### raw
The `raw` parser performs no additional steps and just passes the message on untouched.

### json
The `json` parser parses the input as a json and fails if the data is invlaid.

## Classifier
Classifiers handle message classification based on rules.

### constant
The `constant` classifier classifies all messages with the type passed as it's config.

### mimir
The `mimir` classifier uses the mimir matching language to match rules against given classifications.

The configuration is provided as a json in the form: `[{"rule1": "classification1"}, {"rule2": "classification2"}, ...]`. If no rule matches the classificatin is set to `"default"`.

## Grouping
The grouping defines the algorithm used to group classified messages.

### pass
The `pass` grouping will pass all messages.

### drop
The `drop` grouping will drop all messages.

### bucket
The bucket grouper allows for rule based rate limiting, for each classification you can specify a limit in messages per second that will be applied using a sliding window with 10ms granularity. buckets are defined by: `<window size in ms>;<sub windows>;<name>:<rate / window size>|<name>:<rate / window size>|...`.

## Limiting
The limiting plugin is responsible for limiting the entierty of the message stream after grouping has been performed.

### pass
The `pass` limiter will pass all messages.

### drop
The `drop` limiter will drop all messages.

### percentile
The `percentile` limiter will keep a percentage of messages. The percentage is provided as a fraction passed to it's config. `0` equals the `drop` limiter, `1` equals the `pass` limiter and `0.9` would mean 90% of the messages pass.

It is possible to also provide 3 more values, `low limit`, `high limit` and `adjust`. If those are provided the percentile will adjust to keep the the feedback between low and high limits, where lower is better and higher is worse. Every time the feedback is either `lower limit` then `adjust` will be added to the `percentile` untill it reaches `1` (`100%`). If the feedback exceeds `high limit` then `adjust` will be substracted from `percentile` until it reaches a `adjust` as a lowst value.

### windowed
The `windowed` limiter limits the total message flow within a given timne window based on a sliding window algorithm. It can be confifgured with `<time range>:<sub windows>:<rate>`. `1000:100:500` Would mean we have a window of `1000ms` (`1s`) split into `100` buckets and allow `500` messages during this timeframe.

It is possible to also provide 3 more values, `low limit`, `high limit` and `adjust`. If those are provided the rate will adjust to keep the the feedback between low and high limits, where lower is better and higher is worse. Every time the feedback is either `lower limit` then `adjust` will be added to the `rate`. If the feedback exceeds `high limit` then `adjust` will be substracted from `rate` until it reaches a `adjust` as a lowst value.

## Output
The output plugin defines the destination the data is forwarded to.

### stdout
The `stdout` output writes messages to stdout, one message per line.

### kafka
The `kafka` output writes messages to a kafka topic, this is configured in the format `<topic>|<broker list>`.

### debug
The `debug` output prints a list of classifications and pass and drop statisticins for them every second.

## es

The `es` output sends data to Elastic search using batch insers, it is configured wiht  `<endpoint>|<index>|<batchSize>|<batchTimeout>`. This output provides feedback based on the batch insert times.

# Docker

The docker container build takes environment variables for each plugin. The `input` plugin is provided as `INPUT=...` and it's configuration as `INPUT_CONFIG=...`. Plugins and configs may be omitted and will then be replaced with a default, for configurations the default is an empty configuration.

In addition the `RUST_LOG` environment variable can be passed to define the log level of the application.

## Tests

Tests can be found in `docker/goss.yaml`

# Local demo mode

Docker needs to have at least 4GB of meomry.

You need to be connected to the VPN.

To demo run `make demo-containers`  to build the demo containers and then `make demo-run` to run the demo containers.

To [demo with elasticsearch and kibana](#elastic-demo) 'make demo-elastic-run'. The 'demo-run' target does not run elsticsearch or kibana. In addition a full [kitchen sink demo](#kitchen-sink-demo) that also outputs data to influx and provides a  .


## Design

The demo mode logically follows the flow outlined below. It reads the data from data.json.xz, sends it at a fixed rate to the `demo` bucket on kafka and from there reads it into the tremor container to apply classification and bucketing. Finally it outputs statistics of the data based on those steps. 

![flow](docs/demo-flow.png)

## Configuraiton

### Config file

The demo con be configured in the `demo/demo.yaml` file. A abbriviated version (with the critical elements) can be seen below. In the following sections we'll quickly discuss each of the configuraiton options available to customize the demo.
```
version: '3.3'
services:
  # ...
  loadgen:
    # ...
    environment:
      - MPS=100
      # ...
  tremor:
    # ...
    environment:
      - CLASSIFIER_CONFIG=[{"short_message=info OR short_message=info":"info"},{"short_message=ERROR":"error"}]
      - GROUPING_CONFIG=1000;100;default:90|info:10|error:100
      # ...
```

### Load Generator

#### MPS

The rate at which data is generated can be configured in the `demo/demo.yaml` file in the `loadgen` section under the `MPS` (metrics per second) variable. How high this can be set depends on the system running the demo.

### Tremor

The normal tremor container is used for testing, while all the variables described above can be configured most of them **should not be changed**. The variables that can safely be changed will be listed below with a short explenation.

#### `CLASSIFIER_CONFIG`

The mimir classifier config, two demo rules are included but further rules to match the `demo/data.json.xz` test data can be added.

#### `GROUPING_CONFIG`

The configured classification / bucketing rules, they should match the classifications defined in `CLASSIFIER_CONFIG`. By default the grouping limiter is configured to use a sldiging window of `100` sub windows over a `1000` ms (`1s`) interval.

### Test data

The test data is read from the `demo/data.json.xz` file. This file needs to contain 1 event (in this case a valid json object) per line and be compressed with `xz`. Changing this document requires re-running `make demo-containers`!

### Elastic demo

The base tremor demo can be extended to include elasticsearch + [kibana](#kibana) via:

```
make demo-elastic-run
```

This exposes elasticsearch on localhost port 9200 and [kibana](#kibana) as documented in it's section.

### Kitchen Sink demo

The kitchen sink adds InfluxDb, Telegraf and Grafana to the base tremor demo with ElasticSearch and Kibana

```
make demo-all-run
```

To inject grafana dashboards and configure influxdb for monitoring bootstrap grafana and influx
once the system stabilizes. Make sure to install the [Demo Tools](#demo-tools)  first!

```
make demo-all-bootstrap
```

#### Grafana
You can access [Grafana](http://localhost:3000/login) with the credentials `admin`/`tremor`. Navigate to the `Tremor Demo` dashboard.


#### Kibana
You can access [Kibana](http://localhost:5601/app/kibana). To use it first set up a new index under *Manage* -> *Index Patterns*. The pattern should be `demo` and the time filter should be set to `I don't want to use the Time Filter`. After saving navigate to *Discover*.


### Demo tools

The influx client and telegraf can be installed locally for dev insights into the demo experience as follows:

```
brew install influxdb
brew install telegraf
```

Only telegraf is required to run the demos, the influx cli is optional ( and ships with influxdb on OS X in homebrew )
