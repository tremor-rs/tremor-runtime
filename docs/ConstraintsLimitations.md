# Constraints and Limitations

This section lists known limitations in the current version of tremor, this limitations are not oversights but tradeoffs driven by the use cases we focused on.

If any of those limitations are prohibitive for you please reach out and we can discuss if tradeoffs can be adjusted

* The Kafka onramp uses automatic acknowledgements for messages.
* The batch operator only flushes based on timeout when a new message arrival.
* Metrics are collected every 10 seconds, but again only when a message arrives at the pipeline. This means if there are no messages (and no new updates) we don't generate metrics.
* The metric output is currently only handling either InfluxDB line protocol or JSON encoding.
* Only incremental backoff is supported by back-pressure.
* Every pipeline, onramp, offramp runs in its own thread.
* Tremor is very opinionated towards performance and not so much towards exploratory work where it is not yet clear what the requirements are.
* The internal data representation is limited to what can be represented in JSON.
* Events that make it through all back pressure and rate limiting mechanism within the pipeline but arrive at a still overloaded offramp are dropped without the option diverting.
* The number of functions is the minimal set to cover current use cases.
* There is no way to 'create' new events within the pipeline.