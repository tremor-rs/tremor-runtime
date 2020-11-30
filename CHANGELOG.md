# Changelog

## 0.9.2

* Standardize error responses for tremor api [#559](https://github.com/tremor-rs/tremor-runtime/pull/559)
* Add docker image, and action for training docker image [#576](https://github.com/tremor-rs/tremor-runtime/pull/576)
* Fix Kafka SmolRuntime hang [#558](https://github.com/tremor-rs/tremor-runtime/pull/558)
* Remove array access to prevent possible runtime panics [#574](https://github.com/tremor-rs/tremor-runtime/pull/574)
* Update CLI: -i allows selecting a subset now [#580](https://github.com/tremor-rs/tremor-runtime/pull/580)
* Allow using err for errors in tremor run [#592](https://github.com/tremor-rs/tremor-runtime/pull/592)
* Performance enhancements [#608](https://github.com/tremor-rs/tremor-runtime/pull/608)
* Fix possible crashes in from todo macro from tcp sink [#573](https://github.com/tremor-rs/tremor-runtime/pull/573)
* Fix linked offramp not shutting down after binding is deleted. [#582](https://github.com/tremor-rs/tremor-runtime/pull/582)
* Fix slow kafka sink when queue.buffering.max.ms is set to > 0. [#585](https://github.com/tremor-rs/tremor-runtime/pull/585)
* Fix string and heredoc errors with and without interpolation. [#595](https://github.com/tremor-rs/tremor-runtime/pull/595)
* Add hygienic error feedback for `tremor run` [#620](https://github.com/tremor-rs/tremor-runtime/pull/620)

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
