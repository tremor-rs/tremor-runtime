# History

Tremor started with a straight forward problem statement: 

**During peak events logs and metrics going to Elastic Search and InfluxDB back up in Kafka queues  removing visibility from the system.**

## tremor-0.4 (stable)

This release combined the lessons from the 0.3 and 0.2 looking at what worked in one and the other. The 0.4 release kept the dynamic pipelines but implemented them in a way closer to how they were implemented in 0.2 retaining the performance this way.

Also,  [contraflow](../Overview#contraflow) introduced in 0.3 was extended with [signals](../Overview#signalflow) to allow non-event carrying messages to move through the pipeline for operational purposes such periodic ticks.

The matching language of the earlier releases got a complete overhaul becoming a more powerful scripting language - [tremor-script](../tremor-script). Tremor script introduced features such as event metadata variables to drive operator behavior outside of the script itself, mutation of event values, support for functions, along with a return statement that allows early returns from a script to save execution time.

The basic idea of a `yaml` file as configuration was carried over from 0.3 but the content dramatically altered to be more usable. Along with the new syntax also the ability to run multiple pipelines, onramps and offramp in the same tremor instance were introduced.

With the new config tremor, 0.4 also introduced an API that allows adding, remove and alter the components running in an instance without requiring a restart. This feature came with the addition of [tremor-cli](../CLI) to expose this API to operators without requiring to remember the interface details.

## tremor-0.3 (develop)

The limitations of static steps imposed run 0.2 were a real limitation. With the 0.3 release tremor got the capability to run arbitrarily complex pipelines of interconnected nodes, along with an improved set of features in the matching language.

Along with that, it introduced the ability to bridge asynchronous and synchronous inputs and outputs allowing for new combinations of on- and Off-Ramps.

The most notable addition to the 0.3 version of tremor, however, was [contraflow](../Overview/#contraflow), a system that allowed us for downstream nodes to traverse the graph in reverse order to communicate back metrics and statistics. This allowed generalising the concept of back pressure from 0.2 and applying it in different places of the pipeline.

With the dynamic pipelines, the configuration also went away from arguments passed to the command line to a `yaml` file that carried the specification of the pipeline which made it easier for an operator to maintain the pipeline.

Those additions and the exploratory nature of the 0.3 released reduced performance by approximately the factor of 2 in this release.

## tremor-0.2 (stable)

With copy as the basis, the next step was what best could be described as an MVP. A bare minimum implementation that was good enough to serve the immediate need and form the foundation for going forward.

The 0.2 release of tremor consisted of a set of static steps that were configured over command line arguments. It handled reading data from Kafka, writing data to Elastic Search and InfluxDB. It included a simplistic classification engine that allowed limiting events based on an assigned class. Also, last but not least a method for handling downstream system overload and back-pressure by applying an incremental backoff strategy.

It solved the problem initially presented - during the next peak event there was no lag invisibility into metrics or logs. And not only did it work it also reduced the computer footprint of the system it replaced by 80%.

## kopy

From this tremor started to build. Its root was a tool called `kopy` (short for `k(afka-c)opy` ) that, given a Kafka queue to copy from and one to copy to, would replicate the data from one to the other.

`kopy` itself was far from sophisticated, but it was good enough to verify the idea of building a tool to solve the problem mentioned above in rust. It served us through the first iteration as the tool we used to collect test data and move it into private queues for replaying.
