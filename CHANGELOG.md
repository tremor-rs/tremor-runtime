# Changelog

## 0.10.2

### Fixes

* Ensure blaster sends all events from the source [#759](https://github.com/tremor-rs/tremor-runtime/pull/759)
* Allow the use of const and custom functions using const in select queries [#749](https://github.com/tremor-rs/tremor-runtime/issues/749)
* Print hygenic errors when invalid `trickle` files are loaded in `server run -f ...` [#761](https://github.com/tremor-rs/tremor-runtime/issues/761)
* Ensure `elastic` offramp does not issue empty bulk requests.
* Avoid sending empty batches from the `batch` operator.
* Ensure `elastic` offramp includes event `payload` in every error response.

### New features

* Add discord connector to allow communicating with the discord API

## 0.10.1

### Fixes

* Update tremor-value to 0.2 to include binary changes and thus unbreak the 0.10 tremor-script crate

## 0.10.0

### Breaking Changes

* String interpolation is now done via `#{..}` instead of `{}`. `{` no longer needs to be escaped, but `\#{` needs an escape for literal `#{`.
* Emit tumbling windows based on `size` immediately when they are full. [#731](https://github.com/tremor-rs/tremor-runtime/pull/731)
* Emit tumbling windows based on time `interval` also when no event comes in but the interval passed [#731](https://github.com/tremor-rs/tremor-runtime/pull/731)
* Elasticsearch offramp `elastic`: change the config value `endpoints` to `nodes` [#732](https://github.com/tremor-rs/tremor-runtime/pull/732)

### New features

* Default to thin-lto for all builds (prior this was only done in docker)
* Automatically generate rpms and tarballs for releases.
* Update rust to 1.49.0
* Build deb packages
* Statically link openssl
* elastic sink now supports linked transports [#715](https://github.com/tremor-rs/tremor-runtime/pull/715)

### Fixes

* rewrite string interpolation to fix [#726](https://github.com/tremor-rs/tremor-runtime/issues/726)

## 0.9.4

### New features

* Extract simd_json::BorrowedValue into tremor specific tremor-value to allow extension of the type system.
* Introduce Binary datatype and binary semi literals `<< 1, 2, event, (3 + 7) >>`
* Introduce `base64` module for encoding and decoding binary to base64 encoded strings
* introduce `binary` module to work with binary data

### Fixes
* Terminate pipeline creation when a node already exists with the given name [#650](https://github.com/tremor-rs/tremor-runtime/issues/650)
* Fix visibility of pipeline metrics [#648](https://github.com/tremor-rs/tremor-runtime/pull/648)
* Fix panic upon usage of postgres ramps due to incompatbility with tokio and async-std runtime. [#641](https://github.com/tremor-rs/tremor-runtime/pull/641)
* Fix missing newlines when error line doesnt end with a newline [#676](https://github.com/tremor-rs/tremor-runtime/pull/676)
* Fix possible panic in random::integer and random::float when used with values <= 0. [#679](https://github.com/tremor-rs/tremor-runtime/pull/679)

## 0.9.3

### New features

* Add `prefix` and `raw` config options to `stdout` and `stderr` offramps [#637](https://github.com/tremor-rs/tremor-runtime/pull/637)


### Fixes

* Make use of postprocessors in `stdout`, `stderr` and `udp` sinks [#637](https://github.com/tremor-rs/tremor-runtime/pull/637)
* Allow to express minimal value of i64 as int literal [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
* Fix scientific float literals (e.g. `1.0e-5`) [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
* Output errors to stderr on `tremor run` invocation. [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
* Output more helpful errors for runtime errors (like accessing non-existing fields) [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
* Fix gelf preprocessor to accept valid (unchunked) gelf messages [#628](https://github.com/tremor-rs/tremor-runtime/pull/628)
* Fix memory access issue for large objects


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
