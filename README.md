This tool allows to configure a pipeline that moves data from a source to a destination. The pipeline consists of multiple steps which are executed in order. Each step is configurable on a plugin basis.

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
The bucket grouper allows for rule based rate limiting, for each classification you can specify a limit in messages per second that will be applied using a sliding window with 10ms granularity. buckets are defined by: `<name>:<rate / s>|<name>:<rate / s>|...`.

## Limiting
The limiting plugin is responsible for limiting the entierty of the message stream after grouping has been performed.

### pass
The `pass` limiter will pass all messages.

### drop
The `drop` limiter will drop all messages.

### percentile
The `percentile` limiter will keep a percentage of messages. The percentage is provided as a fraction passed to it's config. `0` equals the `drop` limiter, `1` equals the `pass` limiter and `0.9` would mean 90% of the messages pass.

## Output
The output plugin defines the destination the data is forwarded to.

### stdout
The `stdout` output writes messages to stdout, one message per line.

### kafka
The `kafka` output writes messages to a kafka topic, this is configured in the format `<topic>|<broker list>`.

# Docker

The docker container build takes environment variables for each plugin. The `input` plugin is provided as `INPUT=...` and it's configuration as `INPUT_CONFIG=...`. Plugins and configs may be omitted and will then be replaced with a default, for configurations the default is an empty configuration.

In addition the `RUST_LOG` environment variable can be passed to define the log level of the application.

## Tests

Tests can be found in `docker/goss.yaml`
