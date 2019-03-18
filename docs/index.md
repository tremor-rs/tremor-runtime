# Tremor

Tremor is an event processing system designed primarily for the needs of
Wayfair Operations.

Tremor has been running in production since October 2018, processes 6
terabytes of data per day, or in the region of 10 billion log messages
per minute and 10 million metrics per second.

Tremor runs 24x7 365 days per year and is implemented in the Rust programming
language.

## Audience

Tremor is designed for high volume messaging environments and is good for:

* Man in the middle bridging - Tremor is designed to bridge asynchronous upstream sources with synchronous downstream sinks. More generally, tremor excels at intelligently bridging from sync/async to async/sync so that message flows can be classified, dimensioned, segmented, routed using user
 defined logic whilst providing facilities to handle back-pressure and other hard distributed systems problems on behalf of its operators.

  * For example - Log data distributed from Kafka to ElasticSearch via Logstash can introduce significant lag or delays as logstash becomes
    a publication bottleneck. Untimely, late delivery of log data to the network operations centre under severity zero or at/over capacity conditions reduces our ability to respond to major outage and other byzantine events in a timely fashion.

* In-flight redeployment - Tremor can be reconfigured via it's API allowing workloads to be migrated to/from new systems and logic to be
  retuned, reconfigured, or reconditioned without redeployment.

* Simple event processing - Tremor adopts many principles from DEBS ( Distributed Event Based Systems ), ESP ( Event Stream Processor ) and the CEP ( Complex Event Processing ) communities. However in its current state of evolution tremor has an incomplete feature set in this regard. Over time tremor MAY evolve as an ESP or CEP solution but this is an explicit non-goal of the project.

Tremor currently does not:

* Operate as a distributed system, cluster or grid. This is planned for Q2'19.

* Work on the Microsoft Windows Platform.

## Contact

The tremor team is available on slack at __#tremor-forum__ during ordinarily during Berlin business hours.
