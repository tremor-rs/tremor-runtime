# Logstash Benchmark

This section documents how to setup logstash for benchmarking to
compare against tremor-script.

## Logstash

Assert that JDK 8 or higher is on your system path

```bash
$ java -version
java version "1.8.0_192-ea"
Java(TM) SE Runtime Environment (build 1.8.0_192-ea-b04)
Java HotSpot(TM) 64-Bit Server VM (build 25.192-b04, mixed mode)
$ javac -version
javac 1.8.0_192-ea
```

Assert that JRuby 9.2 or higher is on your system path

```bash
$ jruby -v
jruby 9.2.6.0 (2.5.3) 2019-02-11 15ba00b Java HotSpot(TM) 64-Bit Server VM 25.192-b04 on 1.8.0_192-ea-b04 +jit [darwin-x86_64]
```

Assert that the `rake` and `bundler` ruby tools are installed

```bash
$ gem install rake bundler
Fetching: rake-12.3.2.gem (100%)
Successfully installed rake-12.3.2
Fetching: bundler-2.0.2.gem (100%)
Successfully installed bundler-2.0.2
2 gems installed
```

Clone and build logstash and its benchmark tool

```bash
$ git clone https://github.com/elastic/logstash
$ cd logstash
$ gradle clean assemble
$ cd tools/benchmark-cli
$ gradle clean assemble
```

# Benchmark methodology

In the first ( this ) version, this was a manual process

## Identify performance related tunables for logstash

Number of logstash workers ( default 2 )

Batch size for micro-batching queues in logstash workers ( default 128 )

## Identity baseline benchmark

Literally named baseline and is a equivalent in functionality to tremor's `empty-passthrough-json` benchmark

## For each logstash tunable, run a benchmark

Given the baseline logstash benchmark and record [results](./logstash-baseline-log.txt) and initial [analysis](./logstash-baseline-log-analysis.csv)

Document command line arguments required to execute each run/iteration

```bash
$ java -cp build/libs/benchmark-cli.jar org.logstash.benchmark.cli.Main \
      --testcase=baseline --distribution-version=5.5.0 \
      --ls-workers <num-workers> \
      --ls-batch-size <batch-size>
```

## For each tremor tunable, run a benchmark

Given the `empty-passthrough-json` equivalent benchmark

Tremor has no performance related tunables

Tremor's benchmark framework has no performance related tunables for the `empty-passthrough-json` equivalent benchmark

Record [results](./tremor-baseline-log.txt)

# Benchmark conditioning and environment

The `benchmark-cli` tool that ships with logstash suffers from a number of issues.

The number of events in each run is fixed and limited to 1 million events per run.

No accomodation is made for warmup to ensure that the JVM has reached a stable state
before results recording begins.

The framework also incorrectly terminates after each run once 1 million results
have been submitted. It should not complete until all workers have drained their
respective queues and the benchmark reaches a quiescent state. Quiescence is not
asserted.

This means that the number of recorded processed events can be less than the configured
target by a significant margin. As these are *micro-benchmarks* and we are concerned with
Order of Magnitude differences in performance we have not expended effort resolving these issues.

A further issue with the logstash `benchmark-cli` tool is that the results suffer from the coordinated 
ommission problem.

The coordinated ommission problem, is a term first-coined by Gil Tene based on his observations in benchmarking the Azul Vega
hardware and Zing JVM with C4 garbage collector and related ZTS subsystems). In a nutshell, coordinated-ommission is where a
benchmarking tool ( usually unintentionally ) incorrectly records events under measurement time spans by failing to record
the intended verses actual time to record. Specifically, it is insufficient to record the start time and end time of a particular
event of interest. Capturing the start and end time allows the delta or servicing time to be computed. It does not capture any
synchronization overhead, waiting time, delays or other system induced hiccups introduced between up to the point the event
should have started. We fail to capture unintentional drift introduced artificially by the benchmark framework when CO is in
force. Well-designed benchmarks should be CO-free.

Tremor's benchmarking facility allows events to be injected at a fixed frequency. This simple tactic ( which more specifically,
pins the intended commencement time for an event to begin processing to a starting epoch ) is sufficient to practically account
for any hiccups or drift in expected verses actual inter-arrival based on designed constraints in a benchmark framework. By selecting
a fixed static frequency any measured hiccups should be outside of the control of the benchmark framework - they are either artefacts
of the scenario under test, or the system upon which it is being tested. These conditions are optimal in all benchmark testing, but
an absolute necessity for any latency-sensitive testing, especially where fine-grained statistic quartiles are being computed if they
are to impart results that are fit for low-level analysis and interpretation.

Coordinated-ommission-free benchmarks are important for micro-benchmarking and latency-driven benchmarking. However, as we are
interested in Order of Magnitude ( finger in the air ) characteristics rather than isolating long-tail latencies or understanding
fine grained latency characteristics on a per-event basis ( say, at the long-tail of performance beyond the 99.99th percentile )

In short, tremor makes some effort to account for coordinated-ommission where relevant, but logstash's benchmark framework does
not. However, as we are taking a 50,000 foot view of characteristic throughput and are not focusing on specific per-event latency
characteristics the identified issues are negligeable for our analysis.

It would be incorrect however, to focus on per-event performance characteristics or focus in on specific latency quartiles or
throughput quartiles to derive any sigificance. Such an analysis would require more effort and would not necessarily deliver
any greater value.

We have not used lab quality environments to run any of the benchmarks. All benchmarks were run on the same development grade
laptop ( not ideal ) with the same background processes active on an intel / Mac OS X x86_64 environment. As such we consider
the results indicative of characteristics and `good enough` for high level analysis.

It should, however, be a small task to follow this report to replicate the characteristic results detailed in this report and
accompanying evidence and to replicate same on similar resources.

# Baseline Analysis

Logstash's `benchmark-cli` was put through 40 variations of its two tunable parameters.

We tested with 1, 2, 4 and 8 logstash workers.


We tested each worker configuration with queue batch size bounds of 1, 2, 4, 8, 16, 32, 64, 128, 256 and 512.

The best of 3 runs was recorded.

As tremor has no tunables we simply recorded the best of 3 runs.

Logstash is configured out of the box for 2 workers and a batch size of 128. There are marginally better configurations
possible with 4 or 8 workers showing marginal throughput benefits given the use-case selected. Configuring a single worker
has a significant negative impact on performance.

As such logstash is well-configured out of the box, at least for typical development or non-production activities.

The optimal configuration on the test machine was configured with 8 workers ( default 2 ) and a batch size of 256 ( default 128 ).
This is less than 1% of a difference. Generally speaking, batch sizes of less than 32 tend to significantly reduce throughput for
any number of configured workers. Also, generally speaking, the improvement from 2 worker threads to more does not demonstrate any
improvement in scaling. As such multi-core scaling with logstash does not seem to be of much benefit beyond 2 workers ( threads ).

It should be noted that the benchmark creates artificial conditions and that the baseline working-set is atypical of production
working sets. On the other hand, this conditioning is the same with respect to tremor whose baseline benchmark is equivalent.

In both cases we simply ingest, forward and publish an event or `pass it through` the system under test.

As logstash is configured for a fixed ceiling of events ( 1 million ) for a benchmark run, and tremor is configured for a specific
test duration ( as many events as possible in 40 seconds ) we need to baseline the results. We bias in favor of tremor as normative
and compute the effective throughput at 40 seconds for each logstash run. So a 63 second run with 8 workes and a batch size of 1
38 seconds run at batch size 256 respectively for logstash is counted as the equivalent 40 seconds run as follows:

|Logstash Default (W2 W128)|Logstash Worst (W1 B1) |Logstash Best ( W8 B256)|Tremor ( baseline )|
|---|---|---|---|
|1033811|466708|1041943|21402853|

Note that we truncate / floor round the equivalent logstash 40-second results for each selected configuration.

Relative to the worst case logstash benchmark run performance:

|Logstash W2 W128 ( default )|Logstash W8 B1|Logstash W8 B256|Tremor ( baseline )|
|---|---|---|---|
|2.22|1|2.23|45.86|

Logstash can itself benefit from at least a 2x improvement, and this is consistent with the default out of the box configuration.

Tremor, however, is a factor of 45 better than the logstash worst case.
Tremor, compared to the logstash best case, is still a factor of 20.54 higher throughput.

So the total effective range of improvement for the given benchmark ( all other things considered equal ) is somewhere
between a 20x to 45x increase in throughput favoring tremor over logstash for the baseline use case based on exprimental
conditions detailed in this report.

# Production Analysis

Of course, we don't typically deploy logstash or tremor into production as a simple distribution proxy or interconnect
that simply passes through events. Logstash would be a pretty bad choice compared to tremor. But tremor, although it
has excellent conditioning and is designed for elegantly handling back-pressure and saturation conditions for log shipping
and distribution - it does not provide the delivery semantics, retention and feature-set of technologies such as Kafka.

For the level 1 traffic limiting, traffic shaping and rate limiting use cases in Level 3 Logging at Wayfair for which tremor
was originally designed we have seen a 7x-8x improvement in density compared to logstash for v0.4 of tremor. v0.5 should increase
this to the 10x ballpark as we benefit from SIMD vectorization of JSON deserialization. However, there is further room for
improvement as the tremor-script langauge has evolved to handle level 2 logging to replace ruby and logstash with far richer
configuration than level 1. In v0.5 performance is a non-goal; as such there are many optimisations to the new tremor-script
domain specific langauge that we have yet to undertake - so in practice for level 2 logging we won't see the full benefit of
SIMD vectorization as some of those gains are ammortised by additional essential complexity of providing a richer DSL to
support replacing logstash in level 2.

Indicatively, we stand to see a range of 20x-40x improvement. In production we have *observed* closer to a 7x-8x improvement
in density with the L3 replacement, and with the v0.4 upgrade to L1 in GCP pre-live. We expect a further modest incremental
improvement in L1 with v0.5, and a good ~10x over logstash for L2 this ( v0.5 ) release.

# Real-World Scenario

TBD - In this section we revisit the 'real-world-throughput' scenario in tremor that reflects the L1 use case in
full-scale production. This needs to be migrated ( as far as is possible, minus rate limiting features ) to a
comparable hypothetically equivalent logstash configuration for benchmark purposes.

# Loggging Level 2 Replacement Scenario

TBD - In this section we compare L2 logstash configuration ( or a subset ) against the equivalent tremor configuration.
