# Pull request

## Description

<!-- please add a description of what the goal of this pull request is -->

## Related

<!-- please include links to related issues are RFCs, remove if not required  -->

* [RFC](https://rfcs.tremor.rs/0000-.../)
* Related Issues: fixes #000, closed #000
* Related [docs PR](https://github.com/tremor-rs/tremor-www-docs/pull/000)

## Checklist

<!--
Please fill out the checklist below.

If an RFC is required and not submitted yet the PR will be tagged as RFC required and blocked
until the RFC is submitted and approved.

As a rule of thumb, bugfixes or minimal additions that have no backwords impact and are fully
self-contained usually do not require an RFC. Larger changes, changes to behavior, breaking changes
usually do. If in doubt, please open a ticket for a PR first to discuss the issue.

-->

* [ ] The RFC, if required, has been submitted and approved
* [ ] Any user-facing impact of the changes is reflected in docs.tremor.rs
* [ ] The code is tested
* [ ] Use of unsafe code is reasoned about in a comment
* [ ] Update CHANGELOG.md appropriately, recording any changes, bug fixes, or other observable changes in behavior
* [ ] The performance impact of the change is measured (see below)

## Performance

<!--
Measure or reason about the performance impact of the change.

A rough indication is sufficient here, we often use the `real-world-json-throughput` example to

1. cargo build -p tremor-cli --release (on main)
2. ./bench/run real-workflow-throughput-json
3. repeat on main

Share the two throughput numbers and the benchmark that produced them.

NOTE: We are fully aware this is not a perfect method, but it is a tradeoff between preventing large
performance degradation and putting an unreasonable burden on contributors.

NOTE: the total number is irrelevant as it will vary from system to system. The delta is what
matters.

If the benchmarks fail on your system, note it in the issue, and someone will try to run them for
you.

If your changes do not affect performance, and you can argue this, do it in this section, it is a
valid response.

-->


