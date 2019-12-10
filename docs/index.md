# Tremor

Tremor is an event processing originally system designed primarily for the needs of Wayfair Operations.

Tremor has been running in production since October 2018, processes 6 terabytes of data per day, or in the region of 10 billion log messages per minute and 10 million metrics per second.

Tremor runs 24x7 365 days per year and is implemented in the Rust programming language.

Click for an [Architectural overview](./overview.md) or [Canned History](./history.md) of the project.

Other interesting topics are:

* [The tremor-script language](tremor-script/index.md)
* [The tremor-query language](tremor-query/index.md)
* Artefacts namely:
  * [Onramps](artefacts/onramps.md)
  * [Offramps](artefacts/offramps.md)
  * [Codecs](artefacts/codecs.md)
  * [Pre-](artefacts/preprocessors.md) and [Postprocessors](artefacts/postprocessors.md)
* Operational information about
  * [Monitoring](operations/monitoring.md)
  * [Configuration](operations/configuration.md) and the [Configuration Walkthrough](operations/configuration-walkthrough.md)
  * [The tremor CLI](operations/cli.md)
* [The tremor API](api.md)
* Development related information
  * [Benchmarks](development/benchmarking.md)
  * [A Quickstart Guide](development/quick-start.md)
  * Nots about [Testing](development/testing.md) and [Debugging](development/debugging.md)

This is not an exhaustve list and for the curious it might be worth to explore the `docs` forlder on their own.

## Contact

The tremor team is available on slack at __#tremor__  or __#tremor-forum__ ordinarily during Berlin business hours.
