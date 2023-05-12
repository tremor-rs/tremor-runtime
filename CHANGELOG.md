# Changelog

## [Unreleased]

## Breaking Changes

* All google connectors now require `token` to be either set to `{"file": "<path to json>"}` or `{"json": {...}}`
## [0.13.0-rc.12]

### Fixes

* Fix parsing with `std::datetime::formats::RFC3339` to allow for `Z` (zulu) timezones.

### New features

* Add configuration option `path_style_access` to `s3_streamer` and `s3_reader` connectors.
* Replace `async-std` runtime wint `tokio`

## [0.13.0-rc.11]

### New Features

* Update to rust toolchain 1.68
* Warning types: now warnings can specify what they are about (performance, behaviour, etc.)
* New warnings for consistency, behaviour and performance
* Functions now can create warnings
* Allow grouping of use with `use std::{m1, m2}`
* Add `tremor` codec for tremor to tremor communication
* Optimized buffer handling for json and binflux codec
* Update HTTP connector to allow configured for mime codecs

### Fixes
* Fix google pub sub default api endpoint
* Handle `routing` and `pipeline` information for each bulk item in the `elastic` connector

### Breaking Changes

* **all** std library constants are now uppercase
* Remove `default` in `match of` and `fn of` and standardize on `case _`
* `textual-length-prefix` pre and postprocessor are now named `textual-length-prefixed` for consistency in naming
* `json-sorted` codec  is now part of the `json` codec with `mode: sorted`

## [0.13.0-rc.10]

### New Features

* Added `state` type window where both tick and event can be handled in script form
* Added new config `socket_options` to UDP, TCP and Websocket connectors to enable use of socket option `SO_REUSEPORT`
* Added the `chunk` postprocessor for creating payloads as close to a configurable `max_bytes` as possible
* Arithmetic expressions now error on overflows
* Rust based erlang nif for tremor-script EQC testing

### Fixes

* Fix `udp_client` connector connecting to IPv6 hosts.
* Fix `json` codec to deduplicate duplicate keys by using the last key in the JSON record.
* Fix scoping for named scripts, they no longer polute/share the outer scope.

### Breaking Changes

* Move `no_delay` config of `tcp_server` and `ws_server` to `socket_options.TCP_NODELAY`.
* The HTTP connectors now work with the `mime_mapping` instead of `codec`.

## [0.13.0-rc.9]

### New Features

* Added `std::datetime` library for parsing, formatting and working with datetimes

## [0.13.0-rc.7]

### New Features

* implement script enhancement RFC (initial state and port based scripts)

### Fixes

* Fix bug in local variable resolving when using expression-paths

### Performance

* Improve dogstatsd performance
* Improve performance of `std::array::flatten` by roughly 30%


## [0.13.0-rc.6]
### Fixes

- Fix windows based on `interval` not sending the correct event id when timing out

## [0.13.0-rc.5]
### Fixes
- The Google BigQuery connector will now split requests that are longer than the allowed limit (10MB)

### New Features
- The GoogleBigQuery connector now accepts `table_id` metadata, to allow setting target table per event.

## [0.13.0-rc.4]

### Fixes

- Fix pipeline DAG creation when pipeline has unused nodes (e.g. a stream, operator or script)
- Fix possible hangs in `http_client`, `kv` and `dns` connectors when no pipelines is connected to their `out` port
- Fix `gbq_writer` swallow errors for requests that have been successfully sent to gbq, but failed.
- Fix systemd startup script

## [0.13.0-rc.3]

### New features

- Add `dogstatsd` codec for Datadog DogStasD implementation

### Fixes

- Fix output of `dns_client` being empty for any lookup.

### Breaking Changes

- Remove `$elastic._type` field from `elastic` connector response events

## [0.13.0-rc.2]

### Fixes
- Add clear error messages for pipelines for console in, pipeline out, pipeline in
- Added a new port in ConnectInput and weaved them together
- Added a new port in ConnectOutput and weaved them together
- Fixed linking pipelines to pipelines
- Fixed error reporting in ConnectOutput and ConnectInput functions
- Fixed kv test cases

## [0.13.0-rc.1] 

### Breaking Changes

- Change `gcs_streamer` and `s3_streamer` to only ack events upon finished uploads.
- `cb` connector config `path` changed to `paths`.
- Introduce breaking change in `kafka_consumer` configuration, by adding `mode`.
- Update grok to 0.2, optional patterns are now omitted no longer ""
- Unify to `url` instead of `endpoint` for google and s3 connectors so they're in line with every other connector
- Unify to `urls` for `elastic` connector
- Unify to `path` for `bench`, `kv` and `wal` connector so it's in line with file
- Rename `s3_writer` to `s3_streamer`

### New features

- Add `gcl_writer` Google Cloud Platform Cloud Logging connector for writing log entries, with in-flight concurrency support.
- Add reverse functions and tests for arrays and strings, add sort test for arrays
- Add a ClickHouse connector
- Add a GCS streamer connector

### Fixes

- Fix `tcp_server`, `ws_server` and `unix_socket_server` to properly close sockets when a client disconnects.
- Fix bug in the runtime, swallowing some event acknowledgements and fails for batched events from multiple streams
- Fix custom metrics for `kafka_consumer`, `kafka_producer` not being reported in the requested interval
- Fix `metrics` connector not emitting all metrics it did receive.
- Fix invalid scope handling for `flow` definition default arguments
- Fix `elastic` sink not acking events handled successfully if no source is connected.
- Fix `kafka_consumer` possibly committing earlier offsets, thus replaying events that have already been handled.
- Fix off-by-one error in `kafka_consumer` committing offsets, thus replaying the last committed event.
- Allow `kafka_consumer` connector to reconnect upon more error conditions and avoid stalls.
- Include flow alias in pipeline and connector aliases reported via metrics events and logging in order to deduplicate entries

## [0.12.4]

### Fixes
- Fix startup startup script for systemd installations
- Avoid hangs in `elastic` connector when not connected as source via its `out` or `err` port.
- Default udp client bind ip to `0.0.0.0:0` instead of `127.0.0.1:0`

### New features
- Add compression level for `lz4`, `xz2` and `zstd`

## [0.12.3]
### Fixes
- Don't fail unrecoverably when the connection to the server is lost in Google PubSub consumer.
- Update bert operators to work with tremor 0.12


## [0.12.2]
### New features

- Added the `gpubsub_publisher` connector
- Added the `gpubsub_consumer` connector
- Added new metadata options to `elastic` connector: `version`, `version_type`, `retry_on_conflict`, `if_primary_term`, `if_seq_no`

### Fixes

- Fix race condition leading to quiescence timeout when shutting down Tremor
- Fix `timeout` config unit mismatch in `qos::backpressure` and `qos::percentile` operators. Changed to nanoseconds precision.
- Update WAL operator to fix issue with overwriting first entries.

## [0.12.1]

### Fixes

- Fix misbehaving stdio connector when stdio is a pipe
- Fix Docker entrypoint script argument ordering
- Fix id encoding for discord connector

## [0.12.0]

### New features

- Add full TLS support for `elastic` connector (including client certificate auth)
- Add several auth methods for `elastic` and `HTTP` connector (Basic Auth, Api-Key, Bearer Token, ...)
- Add support for specifying client certificates with `tls` config for `http_client` and `tcp_client` connectors.
- Add `tremor new` to create new template projects.
- Support `elastic.raw_payload` for `update`
- Add support for `chunked` requests and responses for the HTTP connectors
- Add the `gbq` connector for Google BigQuery
- Add support for `tuple patterns` inside of record patterns.
- Refactor visitors to seperate walker and visitor and visit all nodes.
- Add support for modular subqueries in Trickle
- Add `check_topic_metadata` configuration flag to kafka source to bypass topic metadata fetch
- port `json!` improvements to `literal!`
- print names of failing tests
- Add `check_topic_metadata` configuration flag to kafka source to bypass topic metadata fetch
- port `json!` improvements to `literal!`
- Add support for `troy` deployment language. The language adds the `pipeline`, `connector`, `flow`, `links`, `connect`, `to`, `deploy` and `config` reserved keywords. These are now reserved in the scripting and query languages and must now be escaped when they appear in event data. This is a breaking change in the query and script language dialects.
- Add support for `FLOAT4` and `FLOAT8` serialization/deserialization to postgres connectors
- Add `std::path::try_default` fn
- Experimental ARM support
- Add `default => {...}` and `default "key" => "value"` to patch
- Add `zstd` pre- and post-processors [#1100](https://github.com/tremor-rs/tremor-runtime/issues/1100)
- Remove `rental` from `Event` [#1031](https://github.com/tremor-rs/tremor-runtime/issues/1031) [#1037](https://github.com/tremor-rs/tremor-runtime/issues/1037)
- Put event raw payload into `Arc` to improve cloning perf
- Remove `rental` from the entire runtime [#1037](https://github.com/tremor-rs/tremor-runtime/issues/1037)
- Restructure operators and select to avoid transmutation [#1024](https://github.com/tremor-rs/tremor-runtime/issues/1024)
- Fix tremor-cli to use it's own binary when possible [#1096](https://github.com/tremor-rs/tremor-runtime/issues/1096)
- Restructure functions to avoid transmutation [#1028](https://github.com/tremor-rs/tremor-runtime/issues/1028)
- Update blackhole to print events / s [#1129](https://github.com/tremor-rs/tremor-runtime/issues/1129)
- Improve soundness and documentation of SRS code.
- Add support for concatenating arrays [#1113](https://github.com/tremor-rs/tremor-runtime/issues/1113)
- Allow gcp headers to be included in `rest` offramp with `auth: gcp`
- Add env source [#1136](https://github.com/tremor-rs/tremor-runtime/issues/1136)
- Remove need for emit_emtpy_window by automatic eviction for groups where no window holds data
- Ensure all windows of a group are alligned and can not go out of sync
- Box apropriately in the rest sink
- replace macros with functions in gcp code
- Add -q flag and clarify -v flag for unit tests
- Add win::cardinality function
- Backpressure now allows lossless (no discard) circuit breaker behaviour [#1119](https://github.com/tremor-rs/tremor-runtime/issues/1119)
- Allow functions, constants and expressions as roots for path lookups
- Add the `unix-socket` source
- Enabale automatic benchmakrs
- Add the `csv` codec
- Add the `permissions` option for setting the file mode for the `unix-socket` source
- Tests can be run without their suite. [#1238](https://github.com/tremor-rs/tremor-runtime/pull/1283)
- Add the `std::size` module to convert sizes
- Add custom function calls to constant folding
- Integration test names are added as a tag so they can be run by name
- integration tests low log stdout/stderr for before and after

### Breaking Changes

- the `tremor api` sub command in the cli has been removed.
- the `-` is no longer a valid part of identifiers.
- binaries now use `_` to sepoerate type names as `-` is no longer a identifier.
- changed naming for `record` object to avoid keywords like `select` and `merge`. New names are `record.extract` and `record.combine`.
- command separators are now unified, both `patch`, `match` and `for` now use `;` the same way the rest of the language does
- in all definitional statements `args` now specifies interface arguments that are overwritable in the correspanding `create` statement, while `with` specifies non-overwritable configuration in both `define` and `create` statements - this unifies the use of `with`  and `args` between trickle and troy
- file connector no longer splits by lines - it now requires a preconnector
- define for both troy and trickle now follow the same principle of `define <type> <alias> from <source>`
- `wal` is no longer an operator but a connector
- for the elastic connector indexes have not to be set on the batch not the individual event so one batch can only be to a single index.
- metronome interval is now in nanoseconds (as all other timings)
- Most connectors require a specified codec now instead of using JSON as a default
- `merge` no longer treats `null` in the spec as a delete option but rather as a normal value
- Combine all compression and decompression pre/postprocessors.

### Fixes

- Fix detection of `*.troy` files in entrypoint.sh causing duplicate configs to be loaded when using Kubernetes.
- Fix a one-off error in the `bench` connector leading to it producing one event too much
- Avoid acking events that failed while preprocessing or decoding using a codec.
- Remove acknowledging events when dropped or sent to a dead end (e.g. unconnected port) as this was causing confusing and unwanted behaviour with error events
- move EXIT connector to debug connectors to avoid shutting down tremor
- Fix bug in grok extractor that would never return good matches
- Properly terminate after `tremor run ...`
- Fix the `bench` connector to actually stop after the given amount of events when `iters` is configured.
- http headers to allow strings and arrays
- fix the use of `args` in the with part of a `create` insode of a `flow`.
- fix silent swallowing of unknown fields in connector definition.
- Make otel severity_number optional: #1248
- Don't allow duplicate stream names: #1212
- Fix memory safety issue when using `merge` or `patch` with `state` as target and reassigning the resulting value to `state` [#1217](https://github.com/tremor-rs/tremor-runtime/pull/1217)
- Fix delayed event re-execution in case of errors in a branched pipeline
- Skip instead of fail EQC on out of repo PRs
- Ensure patch keys are strings to move runtime errors into the compiletime
- Fix issue with the token `"` being presented as a tick in errors
- Fix `heredoc_start` and `heredoc_end` showing up in error messages instead of `"""`
- Fix some errors in otel pb <-> json translation
- Fix windowed queries emitting events with `null` metadata on tick
- Fix sorting for artefacts
- Fix issue where the test framework would generate reports without being asked for it [#1072](https://github.com/tremor-rs/tremor-runtime/issues/1072)
- Remove the need for eviction_period for time based windows
- Remove dead code and unneeded allows in otel and gcp code
- Fix test and suite names not being printed
- Fix badly nested structure in unit tests
- Fix bug in the unit testing framework that would ignore all tags
- Fix the illogical structure of suites that required a doubly nested record
- Fix `-v` flag
- Fix issue with double counting of unit test stats
- Fix issue with wrong script snippets being shown for unit tests
- Fix argument order in test cases
- Fix GCS go-auth token refresh
- Fix `create script` syntax for aliased scripts with overridden `params`
- Add benchmark names to benchmark tags
- Kafka onramp: Remove failing metadata fetch in order to verify topic existance. Instead detect subscription errors and stop the onramp in that case.
- Unix offramp: Add the missing StartStream message
- tremor-script: Add more details about Unicode in the documentation of the `string` module
- Fix `hdr` and `dds` aggregation function losing events when aggregating > 8192 events
- Ensure merge can only happen on objects

## [0.11.12]

### Fixes
- Unix onramp: Add the missing StartStream message to handle multiple connections correctly

## [0.11.10]

### Fixes

- Fix windowed queries emitting events with `null` metadata on tick

## [0.11.9]

### Fixes

- Kafka onramp: Remove failing metadata fetch in order to verify topic existance. Instead detect subscription errors and stop the onramp in that case.


## [0.11.8]

### Fixes

- Fix `hdr` and `dds` aggregation function losing events when aggregating > 8192 events
- Make otel severity_number optional: #1248

### New Features

- Add the `unix-socket` onramp

## [0.11.7]

### Fixes

- Upgrade dependency on librdkafka to 1.6.1 [#1228](https://github.com/tremor-rs/tremor-runtime/pull/1228).

## [0.11.6]

### Fixes

- Fix possible memory unsafety issue when using patch or merge on `state` [#1217](https://github.com/tremor-rs/tremor-runtime/pull/1217).

## [0.11.5]

### Fixes

- Fix artefact sorting in tremor startup script

## [0.11.4]
- Update to clap 3, this forced some breaking changes:
  - `tremor server run -f file1 file2` now is `tremor server run file1 file2`
  - `tremor test -i i1 i2 -e e1 e2` is now `tremor test -i i1 -i i2 -e e1 -e e2`


### New features

- Add tests for patch feature in tremor-script [#721](https://github.com/tremor-rs/tremor-runtime/issues/721)
- Add onramp for SSE (Server Sent Events) [#885](https://github.com/tremor-rs/tremor-runtime/issues/885)
- Add raw flag in tremor-cli dbg [#854](https://github.com/tremor-rs/tremor-runtime/issues/854)

### Fixes

- Fix the Release archive and package build process
- Replace preprocess from tremor-cli dbg with --preprocess option [#1085](https://github.com/tremor-rs/tremor-runtime/issues/1085)

## [0.11.3]

### New features

- Add tests for merge feature in tremor-script [#721](https://github.com/tremor-rs/tremor-runtime/issues/721)
- Add support for receiving TLS encrypted data via TCP onramp.
- Add support for sending TLS encrypted data via TCP offramp.

### Fixes

- Remove a number of transmutes
- Catalog remaining transmutes with related tickets
- Bump snmalloc-rs to 0.2.26 to ensure builds succeed on systems with libc < 2.25

## [0.11.2]

### New features

- Add `op` key to KV offramp responses in order to differentiate responses by the command that triggered them
- Change format of KV offramp responses to a more unified structure.
- Add `KnownKey::map_*` functions to directly work on the `Value::Object`s inner `HashMap`, if available.
- Add `HEALTHCHECK` to Dockerfiles
- Improve printing for dot files
- Scan sub directories of ${CFG_DIR} for config files.
- Add offramp and onramp for [AMQP](https://www.amqp.org) with the [lapin](https://docs.rs/lapin/1.6.8/lapin/) package.

### Fixes

- KV offramp sends error responses for each failed command
- Ensure binding to first bind linked offramps to pipelines, then pipelines to offramps/onramps/pipeline, then onramps to pipelines to ensure events only start flowing when all downstreams are connected.
- Fix empty record pattern to only match records.
- Fix ws offramp not reconnecting after connection loss.
- Run tests in tremor-cli bin
- Switch operations of `tremor dbg lex` and `tremor dbg preprocess` as they did the job of the other.
- Fix heredoc preprocessing, which was messing up error reporting
- Fix false positives in cycle detection
- Include `cncf::otel` stdlib sources in deb package
- Add `/usr/local/share/tremor` to default `TREMOR_PATH` also for all packages as a well-known directory for custom tremor-script libraries and modules.
- Record the partition number assigned during rebalancing when running Kafka.
- Fix bug in HDR histogram implementation when using emit without reset.
- Fix bug in mean that invalid values would be counted as part of the total number of values.
- Avoid possible contraflow cycles via `system::metrics` pipeline, if a pipeline is connected to an output port of `system::metrics` pipeline.

## [0.11.1]

### New features

- Add `tremor_value::structurize` convenience fn
- Change `qos::wal` operator to only require one of `max_elements` or `max_bytes` (using both is still possible).
- Add DNS sink
- Add syslog codec.
- Add `$udp.host` and `$udp.port` to allow controling udp packet destinations on a per event basis.
- Deprecate `udp.dst_*` config, introduce `udp.bind.*` config instead.
- Allow insights/contraflow events to traverse through multiple connected pipelines

- Add GCP Cloud Storage linked sink connector.
- Add textual-length-prefix pre and postprocessor.
- Add GCP Pubsub sink and source connector.

### Fixes

- Inform docker builders that lack of resources may crash builds
- Fix CI runners to work with caching
- Fix dependencies for `tremor-value` and `tremor-common`
- Fix docker entrypoint not forwarding arguments to tremor binary, unless `--` is used before them
- Fix match default clause only executing the last statement in the block.
- Kafka back to async with a timeout on waiting
- Fix `qos::wal` operator never cleaning up the last event from its storage
- Fix `generic::batch` operator swallowing the `transactional` status of an Event
- Fix windowed select queries not tracking the `transactional` status of an Event
- Fix windowed select queries not tracking the Events that constitute an outgoing aggregated event
- Avoid several offramps to swallow fail insights.
- Send correlation metadata for send error events in elastic sink
- Record the partition number assigned during rebalancing when running Kafka.

## [0.11.0]

### New features

- CNCF OpenTelemetry source, sink and `cncf::otel` tremor-script library adds log, trace, metrics OpenTelemetry support
- Fixes module path function resolution for `tremor-cli doc` tool to use unified path resolution
- Removed the vsn.sh script which checks if the lockfile is up to date and replaces it with the --locked flag [#798](https://github.com/tremor-rs/tremor-runtime/pull/798)
- Allow using '\_' as seperators in numeric literals [#645](https://github.com/tremor-rs/tremor-runtime/issues/645)
- Refactor kafka metadata variables to be under a single record `$kafka`.
- Add support for Kafka message headers, available through the `$kafka.headers` metadata variable.
- Add the `cb` offramp for testing upstream circuit breaker behaviour [#779](https://github.com/tremor-rs/tremor-runtime/pull/779)
- Add the `kafka` onramp config `retry_failed_events` to acoid retrying failed events, and `polling_interval` to control how often kafka is polled for new messages if none were available previously [#779](https://github.com/tremor-rs/tremor-runtime/pull/779)
- Add `kv` connector with the supported operations `put`, `get`, `delete`, `scan`, `cas`.
- Add discord badge to README.md.
- Handle signals and terminate properly on Ctrl+C in docker [#806](https://github.com/tremor-rs/tremor-runtime/pull/806)
- Update to rust 1.50.0
- Emit error events to the `err` port on exceeding concurrent requests limit for `rest` and `elastic` offramps.
- Add the `max_groups` and `emit_empty_windows` settings on window definitions [#828](https://github.com/tremor-rs/tremor-runtime/pull/828)
- Restrict event and event metadata references in the `SELECT` clause of a windowed select statement [#828](https://github.com/tremor-rs/tremor-runtime/pull/828)
- Improve default visibility of tremor info logs from packages [#850](https://github.com/tremor-rs/tremor-runtime/pull/850)
- Include the request info of a response for a linked rest offramp, available through the `$response.request` metadata variable.
- Emit warnings when a window with `emit_empty_windows` without guards is used.
- Extend match to handle top-level `~` extractors in match [#834](https://github.com/tremor-rs/tremor-runtime/issues/834)
- Extend match to allow more assign expressions (i.e. `case a = ~ glob|snot*| =>` or `case v = _ =>`)
- Match pipeline improvements: tree search for `==`, grouping based on shared keys, re-ordering of exclusive case statements
- Optimize glob matches of the form `glob|snot*|` or `glob|*badger|` to cheaper prefix and suffix checks
- Remove warnings for match w/o default if a `_` case or a `v = _` case exists
- Add `--exprs-only` to `dbg ast` to not show metadata
- Add Delete and Update to ES sink [#822](https://github.com/tremor-rs/tremor-runtime/issues/822)
- Add more metadata to Kafka source [#874](https://github.com/tremor-rs/tremor-runtime/issues/874)
- Update to simd-json 0.4
- Add tests covering basic operations and string interpolation for tremor-script[#721](https://github.com/tremor-rs/tremor-runtime/issues/721)
- Add offramp and onramp for [NATS.io](https://nats.io/).
- Add a stdin onramp.
- Add tests covering arrays and records for tremor-script[#721](https://github.com/tremor-rs/tremor-runtime/issues/721)
- Support for non caching UDP sink [#900](https://github.com/tremor-rs/tremor-runtime/issues/900)

### Fixes

- Fix `kafka` onramp hanging with no message in the queue, leading to delayed offset commits [#779](https://github.com/tremor-rs/tremor-runtime/pull/779)
- Fail the `kafka` onramp if any of the configured topics could not be subscribed to [#779](https://github.com/tremor-rs/tremor-runtime/pull/779)
- Tremor no longer requires a home dir for operations that do not need a config [#782](https://github.com/tremor-rs/tremor-runtime/issues/782)
- Add performance section to PR template.
- Fix markdown for discord link.
- Fix lalrpop builds when extra folders exist.
- Refactor operator metrics collection to eliminate clones in most cases.
- Fix systemd spec file to load tremor files in `/etc/tremor/config` [#784](https://github.com/tremor-rs/tremor-runtime/issues/784)
- Do not crash when we can not execute a config file [#792](https://github.com/tremor-rs/tremor-runtime/issues/792)
- Sort the artefacts while running the benchmarks so that the benchmark run is more deterministic [#825](https://github.com/tremor-rs/tremor-runtime/issues/825)
- Remove the bench_pipe_passthrough_csv benchmark [#825](https://github.com/tremor-rs/tremor-runtime/issues/825)
- Fix a bug in the `test bench` command that was giving false negatives when the benchmarks were failing [#816](https://github.com/tremor-rs/tremor-runtime/pull/816)
- Fix time based windows to not emit empty windows anymore and to not error if they contain event references [#828](https://github.com/tremor-rs/tremor-runtime/pull/828)
- Do not commit an empty topic-partition-list in the kafka onramp and improve logging for better debugging
- Fix kafka consumer offset lag of at least `1` continually by using offset + 1 when committing.
- Fix issue where binary not (`!`) was not getting lexed correctly [#833](https://github.com/tremor-rs/tremor-runtime/issues/833)
- Fix missing ack/fail insight events with offramps that dont support guaranteed delivery (e.g. udp, stdout) [#870](https://github.com/tremor-rs/tremor-runtime/pull/870)
- Fix wrong error message for misconfigured UDP sinks
- the `api-host` argument is renamed from `-h` to `-a` to resolve a conflict with `-h` being used for help

## [0.10.2]

### Fixes

- Ensure blaster sends all events from the source [#759](https://github.com/tremor-rs/tremor-runtime/pull/759)
- Allow the use of const and custom functions using const in select queries [#749](https://github.com/tremor-rs/tremor-runtime/issues/749)
- Print hygenic errors when invalid `trickle` files are loaded in `server run -f ...` [#761](https://github.com/tremor-rs/tremor-runtime/issues/761)
- Ensure `elastic` offramp does not issue empty bulk requests.
- Avoid sending empty batches from the `batch` operator.
- Ensure `elastic` offramp includes event `payload` in every error response.

### New features

- Add discord connector to allow communicating with the discord API

## [0.10.1]

### Fixes

- Update tremor-value to 0.2 to include binary changes and thus unbreak the 0.10 tremor-script crate

## [0.10.0]

### Breaking Changes

- String interpolation is now done via `#{..}` instead of `{}`. `{` no longer needs to be escaped, but `\#{` needs an escape for literal `#{`.
- Emit tumbling windows based on `size` immediately when they are full. [#731](https://github.com/tremor-rs/tremor-runtime/pull/731)
- Emit tumbling windows based on time `interval` also when no event comes in but the interval passed [#731](https://github.com/tremor-rs/tremor-runtime/pull/731)
- Elasticsearch offramp `elastic`: change the config value `endpoints` to `nodes` [#732](https://github.com/tremor-rs/tremor-runtime/pull/732)

### New features

- Default to thin-lto for all builds (prior this was only done in docker)
- Automatically generate rpms and tarballs for releases.
- Update rust to 1.49.0
- Build deb packages
- Statically link openssl
- elastic sink now supports linked transports [#715](https://github.com/tremor-rs/tremor-runtime/pull/715)

### Fixes

- rewrite string interpolation to fix [#726](https://github.com/tremor-rs/tremor-runtime/issues/726)

## [0.9.4]

### New features

- Extract simd_json::BorrowedValue into tremor specific tremor-value to allow extension of the type system.
- Introduce Binary datatype and binary semi literals `<< 1, 2, event, (3 + 7) >>`
- Introduce `base64` module for encoding and decoding binary to base64 encoded strings
- introduce `binary` module to work with binary data

### Fixes

- Terminate pipeline creation when a node already exists with the given name [#650](https://github.com/tremor-rs/tremor-runtime/issues/650)
- Fix visibility of pipeline metrics [#648](https://github.com/tremor-rs/tremor-runtime/pull/648)
- Fix panic upon usage of postgres ramps due to incompatbility with tokio and async-std runtime. [#641](https://github.com/tremor-rs/tremor-runtime/pull/641)
- Fix missing newlines when error line doesnt end with a newline [#676](https://github.com/tremor-rs/tremor-runtime/pull/676)
- Fix possible panic in random::integer and random::float when used with values <= 0. [#679](https://github.com/tremor-rs/tremor-runtime/pull/679)

## [0.9.3]

### New features

- Add `prefix` and `raw` config options to `stdout` and `stderr` offramps [#637](https://github.com/tremor-rs/tremor-runtime/pull/637)

### Fixes

- Make use of postprocessors in `stdout`, `stderr` and `udp` sinks [#637](https://github.com/tremor-rs/tremor-runtime/pull/637)
- Allow to express minimal value of i64 as int literal [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
- Fix scientific float literals (e.g. `1.0e-5`) [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
- Output errors to stderr on `tremor run` invocation. [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
- Output more helpful errors for runtime errors (like accessing non-existing fields) [#629](https://github.com/tremor-rs/tremor-runtime/pull/629)
- Fix gelf preprocessor to accept valid (unchunked) gelf messages [#628](https://github.com/tremor-rs/tremor-runtime/pull/628)
- Fix memory access issue for large objects

## [0.9.2]

### New Features

- Add docker image, and action for training docker image [#576](https://github.com/tremor-rs/tremor-runtime/pull/576)
- Performance enhancements [#608](https://github.com/tremor-rs/tremor-runtime/pull/608)

### Breaking Changes

- Standardize error responses for tremor api [#559](https://github.com/tremor-rs/tremor-runtime/pull/559)
- Remove empty event filtering from [lines](https://docs.tremor.rs/artefacts/preprocessors/#lines) preprocessor logic [#598](https://github.com/tremor-rs/tremor-runtime/pull/598)

### Fixes

- Fix possible crashes from todo macro in tcp sink [#573](https://github.com/tremor-rs/tremor-runtime/pull/573)
- Fix linked offramp not shutting down after binding is deleted [#582](https://github.com/tremor-rs/tremor-runtime/pull/582)
- Fix slow kafka sink when queue.buffering.max.ms is set to > 0 [#585](https://github.com/tremor-rs/tremor-runtime/pull/585)
- Fix string and heredoc errors with and without interpolation [#595](https://github.com/tremor-rs/tremor-runtime/pull/595)
- Add hygienic error feedback for `tremor run` [#620](https://github.com/tremor-rs/tremor-runtime/pull/620)
- Fix Kafka SmolRuntime hang [#558](https://github.com/tremor-rs/tremor-runtime/pull/558)
- Remove array access to prevent possible runtime panics [#574](https://github.com/tremor-rs/tremor-runtime/pull/574), [#598](https://github.com/tremor-rs/tremor-runtime/pull/598)
- Update CLI: -i allows selecting a subset now [#580](https://github.com/tremor-rs/tremor-runtime/pull/580)
- Allow using err for errors in tremor run [#592](https://github.com/tremor-rs/tremor-runtime/pull/592)
- Update to rust toolchain 1.48

[0.13.0-rc.1]: https://github.com/tremor-rs/tremor-runtime/compare/v0.12.4...v0.13.0-rc.1
[0.12.4]: https://github.com/tremor-rs/tremor-runtime/compare/v0.12.3...v0.12.4
[0.12.3]: https://github.com/tremor-rs/tremor-runtime/compare/v0.12.2...v0.12.3
[0.12.2]: https://github.com/tremor-rs/tremor-runtime/compare/v0.12.1...v0.12.2
[0.12.1]: https://github.com/tremor-rs/tremor-runtime/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/tremor-rs/tremor-runtime/compare/v0.11.12...v0.12.0
[0.11.12]: https://github.com/tremor-rs/tremor-runtime/compare/v0.11.10...v0.11.12
[0.11.10]:https://github.com/tremor-rs/tremor-runtime/compare/v0.11.9...v0.11.10
[0.11.9]:https://github.com/tremor-rs/tremor-runtime/compare/v0.11.8...v0.11.9
[0.11.8]:https://github.com/tremor-rs/tremor-runtime/compare/v0.11.7...v0.11.8
[0.11.7]:https://github.com/tremor-rs/tremor-runtime/compare/v0.11.6...v0.11.7
[0.11.6]:https://github.com/tremor-rs/tremor-runtime/compare/v0.11.5...v0.11.6
[0.11.5]:https://github.com/tremor-rs/tremor-runtime/compare/v0.11.4...v0.11.5
[0.11.4]:https://github.com/tremor-rs/tremor-runtime/compare/v0.11.3...v0.11.4
[0.11.3]:https://github.com/tremor-rs/tremor-runtime/compare/v0.11.2...v0.11.3
[0.11.2]:https://github.com/tremor-rs/tremor-runtime/compare/v0.11.1...v0.11.2
[0.11.1]:https://github.com/tremor-rs/tremor-runtime/compare/v0.11.0...v0.11.1
[0.11.0]:https://github.com/tremor-rs/tremor-runtime/compare/v0.10.2...v0.11.0
[0.10.2]:https://github.com/tremor-rs/tremor-runtime/compare/v0.10.1...v0.10.2
[0.10.1]:https://github.com/tremor-rs/tremor-runtime/compare/v0.10.0...v0.10.1
[0.10.0]:https://github.com/tremor-rs/tremor-runtime/compare/v0.9.4...v0.10.0
[0.9.4]:https://github.com/tremor-rs/tremor-runtime/compare/v0.9.3...v0.9.4
[0.9.3]:https://github.com/tremor-rs/tremor-runtime/compare/v0.9.2...v0.9.3
[0.9.2]:https://github.com/tremor-rs/tremor-runtime/compare/v0.9.1...v0.9.2
