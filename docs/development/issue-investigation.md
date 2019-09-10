# Investigations

## Tools

Tools for investigating issues in a production environment.

### Logs & Metrics (prod)

Logs and metrics are the first and most fundamental tool when starting to investigate an issue. While they do not provide a deep insight they allow correlating events from a the system under investigation and it is dependants and dependencies.

### `htop` (prod)

Not really a debugging tool but a nice starting point to see values like memory consumption, cpu load both per process and per thread. Things to look out for are:

* Memory consumption
* CPU load
* Number of processes
* System Load ([careful this can be misleading](http://www.brendangregg.com/blog/2017-08-08/linux-load-averages.html)!)

To a degree `top` can be used in its place if `htop` isn't available.

### `perf` (prod: Linux)

Perf is a tool that is used to profile processes, it gives an overview over functions that cpu time is spend on. While it doesn't give a full trace but it is quite handy in situations where we have a hot function to find where a program is spending it time.

It can be run to find where a program spends time:

```bash
perf record -p <pid> # ctrl-c after a while to stop recording
perf report # will pick up the data that record generated
```

It can do **a lot more** than this. A full tutorial on perf can be found [here](https://perf.wiki.kernel.org/index.php/Tutorial).

### `netstat` (prod)

`netstat` can be helpful to investigate networking state of connections and identify connection issues. Again it can be limited in the scope of docker unless executed inside of the container. Please look at the man pages ([`man netstat`](https://linux.die.net/man/8/netstat)) for details, [this tutorial](https://linuxtechlab.com/learn-use-netstat-with-examples/) also covers the basics and useful commands.

It also allows to investigate routing tables which can be very handy for connection issues.

### `ping` (prod)

This isn't exciting but if network related issues are suspected "can I ping this address" can cover a lot of ground and is always worth to check.

This can also be used to investigate MTU

### `lldb` / `lldb-rust`  (prod)

 Both `lldb` and for rust more specifically `lldb-rust` are debuggers. They can attach to a program and step through the application step by step. A good starting guide can be found [here](https://lldb.llvm.org/use/map.html). It needs to be said that using lldb should be done with care, it will stop the program and has the potential to crash or terminate it. So far running lldb against a docker contained often lead to the process hanging or crashing.

### `valgrind` (dev: Linux)

`valgrind` is used to debug and analyse memory leaks. While originally developed for C/C++ it mostly works with rust code as well - however on OS X it has shown to crash code and as the time of writing this is not a usable tool. More information can be found in the [quick start](http://www.valgrind.org/docs/manual/quick-start.html).

### `dtrace` (dev: OS X / Windows / BSD)

`dtrace`is a tool that allows low impact introspection of running systems. It allows putting probes on specific points in either kernel or user land code and perform analytics on the results. It is a very powerful tool but it requires learning to use efficiently. The [dtrace toolkit](http://www.brendangregg.com/dtrace.html) is a good starting point.

### Instruments (dev: OS X)

OS X comes with a user interface around dtrace that abstracts a lot of the complication away and presents some of core functionality in bite sized portions that are easy to use without requiring to understand the whole functionality of dtrace. It is installed alongside with XCode.

Some of the more interesting profiling templates are:

* Leaks - for finding memory leaks
* Time Profiler - for finding 'hot' functions
* System Usage (I/O) - for finding I/O bottle necks

### `strace` (prod: Linux)

`strace` is a tool that allows tracing sys calls in linux, it can be helpful to determine what system calls do occur during a observed issue. For example this can rule in our out specific kernel calls such as IO, muteness, threading, networking and so on.

## Methodology

There is no 'one way' to investigate an issue as what you find during the process will guide the next steps. Having seen and investigated other problems will help as it gives you a set of heuristics to go "oh I've seen this before last time it was X" but it is not required.

However there are general methods that have shown to be efficient in trying to investigate an issue. The mot basic approach consists of three steps:

1. Look at the current state, the indicators that have made the issue visible such as logs or `htop`.
2. Formulate a theory what caused the issue - ideally write it down along with the factors of what promoted you to form this theory.
3. Decide on what would prove your theory - this is where you decide what the next debugging step is, it should ideally either completely confirm, or rule out your theory, that however isn't always possible.
4. If the steps decided on in step 3 fully confirm the issue you're done. If they fully disprove the theory go back to step 1 with the new information. If you neither have prove or disprove for your theory return to 3 and re-formulate what is required to prove it.

Especially initially it helps to document each step as you progress along with any changes you made. This helps to prevent double checking the same theory but also serves as a good learning exercise.

**Note**: Don't be shy to re-visit a theory if you found new evidence for it that were not visible in the first attempt.

**Note**: It is a very helpful to talk to someone, formulating the thoughts in sentences and words often springs new ideas and a second perspective helps with.

## Logs

### Out of memory 'issue' 01

#### Initial observation(source: Metrics & logs)

On one of the busier boxes the memory of the process kept growing until the system started swapping. This happened repeatedly, after restarting the process the memory would slowly grow again.

#### 1st theory: a memory leak in tremor-script

Most of tremor was written in rust which makes it very hard to create memory leaks, however we integrated two pices of C code: tremor script and librdkafka those two pices could either directly or by interfacing with rust introduce memory leaks.

##### disprove of 1st theory (oom)

We ran tremor extensively using the Leak profiler of Instruments to see if it could spot a memory leak. Any amount of memory profiling we did would not show any unusual allocations and missing deallocations. That made the 1st theory unlikely. However this did show a lot of allocation happening inside librdkafka.

#### 2nd theory: Misconfigured librdkafka operations

With the hint gained from the last investigation we looked at the configuration of the system. It showed that a number instances of Kafka onramps were used and the hosts it running un were under spaced regarding to what was communicated to us. Looking at the manual for librdkafka showed that by default each librdkafka instance would allocate up to 1GB of memory as a buffer. Running 4 instances at a 4GB of memory box with additional processes this would eventually lead to running out of memory.

##### prove of 2nd theory

Re-deploying tremor with the librdkafka settings set to only use 100MB of memory for buffers we saw the growth disappear after the expected 400MB proving our theory.

### Infinite loop in librdkafka

#### Initial observation (source: Metrics & logs)

On GCP a node jumped to 75% (100% on a single core) and stopped processing data. No Kafka partitions were assigned to that node until it was restarted.

This bug hunt carries some complications. We have not been able to replicate the bug outside of a production environment. The  environment runs on a outdated linux, has no internet access to download tools and does not have  many of the usual debugging tools available. The bug is rare enough that it take between one and two weeks to reproduce it.

#### 1st theory:  network problems on GCP

Since this was first and only observed directly after  the migration to GCP the initial theory was that networking issues on GCP cause this problem.

##### disprove of 1st theory (lib kafka)

To validate the theory we installed the same version of tremor on premises - and in parallel to working installations and observed if the issue would surface outside of GCP. If it wouldn't we could have reduced it to a GCP related issue.

**Result**: After two weeks of on-prem load we were able to recreate the issue locally, this invalidated our 1st theory.

#### 2nd theory: a bug in our Kafka onramp

Inspecting the code that run Kafka we identified a possible issue that we didn't abort on a bad return value to force a reconnect as we suspected the librdkafak wrapper to handle this situations.

##### disprove of 2nd theory

We patched our Kafka onramp to explicitly handle this bad returns and provide logs in that case (as it should be rare). We updated to see if this would resolve the issue.

**Result**: After a week the issue re-surfaced without the related logs printed.

#### 3rd theory: incompatible versions of librdkafka and kafka

During the update of tremor we also updated the version of librdkakfa - the Kafka version however remained quite old. We theorised that a incompatibility of the newer librdkafka and the old Kafka could cause undefined behaviour such as the busy loop.

##### disprove of 3rd theory

We prepared a build of tremor with the same version of librdkafka we have been using in the past and updated the boxes in question to see if the bug would re-produce.

**Result**: After a week and a half the bug re-surfaced, making a version conflict between Kafka and librdkafka unlikely.

#### 4th theory: unhandled mutex locks in librdkafka

Inspecting a broken process with `perf` we would observe heavy load in pthread related code and the function `rd_kafka_q_pop_serve`

```text
  36.35%  poll          libpthread-2.17.so  [.] pthread_cond_timedwait@@GLIBC_2.3.2
  19.88%  poll          tremor-server       [.] cnd_timedwait
  19.14%  poll          tremor-server       [.] rd_kafka_q_pop_serve
  15.09%  poll          tremor-server       [.] cnd_timedwait_abs
   8.37%  poll          tremor-server       [.] 0x000000000009fcc0
   0.27%  poll          [kernel.kallsyms]   [k] __do_softirq
```

After inspecting [the code](https://github.com/edenhill/librdkafka/blob/v1.0.0/src/rdkafka_queue.c#L336) we noticed that [the lock obtained](https://github.com/edenhill/librdkafka/blob/v1.0.0/src/rdkafka_queue.c#L346) in the function did not check if the lock was successful. The [else condition](https://github.com/edenhill/librdkafka/blob/v1.0.0/src/rdkafka_queue.c#L402-L407) in the function could lead to an infinite loop calling the function over and over again.

##### disprove of 4th theory

We isolated the hot thread using `stop` - looking for the tremor thread that was using 100% CPU. We then traced system calls using `strace`. If librdkafka would attempt to fetch a mutex lock we should see related system calls in the `strace` output.

However observing the process for an hour didn't show a single system call to be made on the hot thread - this ruled out any mutex/kernel related code to be executed in the hot loop.

#### 5th theory: a different rdkafka bug

Inspecting the code and the `perf` output further we noticed that `cnd_timedwait_abs` was part of the [last condition](https://github.com/edenhill/librdkafka/blob/v1.0.0/src/rdkafka_queue.c#L390) in a `while (1)` loop. It is reasonable to assume that if we spend time in `cnd_timedwait_abs` that we hit that part of the loop - if this call would fail the loop would re run possibly creating an infinite loop.

In addition we found a related [kafka issue](https://github.com/edenhill/librdkafka/issues/2208) that pointed to this particular location.

##### proving 5th theory

We deployed half the nodes with a version of tremor that the newest version of librdkafka (1.0.0) that includes a fix for the issue mentioned above while. We expect the patched nodes to keep stable and the unpatched nodes to eventually fail with the bug.

This never failed again.

### UDP GELF messages failed

Issues:

* Undocumented and non standard conform use of Decompress after chunking, this messages were discarded, this was falsly attributed to tremor
* Setup was spanning the WAN using local Logstas and WAN connected Tremor causing MTU issues that was not documented and falsly attributed to tremor
* Using a non standard conform GELF header for 'uncompressed' caused those datapoints to be discarded, this was falsly attributed to tremor
* the UDP buffer on the Tremor and logstash hosts were configured differently causing the tremor host to have signifiantly less buffer causing some messages to be discarded in the OS UDP stack, this was falsly attributed to tremor
* A tool called udp_replay was used to copy data from a logstash host to a tremor host, this tool truncated the udp payload, this paylopad could no longer be decompressed making this packages fail, this was falsy attributed to trmeor
* Some clients send a empty tailing message with a bad segment index (n+1) causing error messages to apear in the logs, this is valid behaviour but were flagged as a 'bug' in tremor because logstash does silently drop those.
* Some clients reuse the sequence number - this can lead to bad messages when UDP packages interleave, tremor reports those incidents and will likely be flagged as buggy because of it.
* MIO's UDP with edge-poll stops receiving data [ticket](https://github.com/tokio-rs/mio/issues/1076) we switched to level poll which solves this.
* PCAP files seem to include lots of invlaid gelfs when replaying
* When replaying the pacap the tool shows the following error.
  thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: Error(WrongField("PacketHeader.incl_len (288) > PacketHeader.orig_len (272)"), State { next_error: None, backtrace: None })', src/libcore/result.rs:999:5
