# Changelog

## Unreleased (in branch main)

* Fix gelf preprocessor to accept valid (unchunked) gelf messages [#628](https://github.com/tremor-rs/tremor-runtime/pull/628)

## 0.9.2

### New Features

* Add docker image, and action for training docker image [#576](https://github.com/tremor-rs/tremor-runtime/pull/576)
* Performance enhancements [#608](https://github.com/tremor-rs/tremor-runtime/pull/608)

### Breaking Changes

* Standardize error responses for tremor api [#559](https://github.com/tremor-rs/tremor-runtime/pull/559)
* Remove empty event filtering from [lines](https://docs.tremor.rs/artefacts/preprocessors/#lines) preprocessor logic [#598](https://github.com/tremor-rs/tremor-runtime/pull/598)

### Fixes

* Fix possible crashes from todo macro in tcp sink [#573](https://github.com/tremor-rs/tremor-runtime/pull/573)
* Fix linked offramp not shutting down after binding is deleted [#582](https://github.com/tremor-rs/tremor-runtime/pull/582)
* Fix slow kafka sink when queue.buffering.max.ms is set to > 0 [#585](https://github.com/tremor-rs/tremor-runtime/pull/585)
* Fix string and heredoc errors with and without interpolation [#595](https://github.com/tremor-rs/tremor-runtime/pull/595)
* Add hygienic error feedback for `tremor run` [#620](https://github.com/tremor-rs/tremor-runtime/pull/620)
* Fix Kafka SmolRuntime hang [#558](https://github.com/tremor-rs/tremor-runtime/pull/558)
* Remove array access to prevent possible runtime panics [#574](https://github.com/tremor-rs/tremor-runtime/pull/574), [#598](https://github.com/tremor-rs/tremor-runtime/pull/598)
* Update CLI: -i allows selecting a subset now [#580](https://github.com/tremor-rs/tremor-runtime/pull/580)
* Allow using err for errors in tremor run [#592](https://github.com/tremor-rs/tremor-runtime/pull/592)
* Update to rust toolchain 1.48
