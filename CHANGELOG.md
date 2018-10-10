# Changelog


## 0.2.3

### Changes

* Add kafka client id w/ hostname and thread to the kafka on and offramp

## 0.2.2

### Changes

* Change injected key for ES from `_tremor` to `tremor`.
* Added link time optimisation
* Build with CPU native target

## 0.2.1

### Action required

* `DROP_OFFRAMP` and `DROP_OFFRAMP_CONFIG` are renamed to `DIVERTED_OFFRAMP` and `DIVERTED_OFFRAMP_CONFIG` respectively in the RPM config. The old settings will work for the time being and removed in a later release.

### Changes

* Add influx offramp with the same batching and backoff as the elastic search offramp. So far filtering is only possible on the `measurement` and `timestamp` field.
* Documented file on- and off-ramp

## 0.2.0

### Action required

* The ES output config removes the field `index` and adds `suffix` instead. If set the `suffix` will be added after the value of `index_key` from the rule but before the date.
* The parser now needs to be set to the right formart, `json` for logstash. This is required to pave the way for multi protocol support like influx.
* `DROP_OFFRAMP` and `DROP_OFFRAMP_CONFIG` are renamed to `DIVERTED_OFFRAMP` and `DIVERTED_OFFRAMP_CONFIG` respectively. The old settings will work for the time being and removed in a later release.

### Changes

* A `GETTING_STARTED.md` is now part of the repository to walk new users through the required steps to get tremor running locally.
* Start keeping a changelog for tracking changs especially those requiering action of the user.
