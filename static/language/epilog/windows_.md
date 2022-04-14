
A comma delimited set of window references.

Windows can be local or modular

```tremor
win, my_module::one_sec, my_module::five_sec
```

The identifers refer to a window definition and can be used
in operators to define a temporally bound set of events based
on the semantics of the window definition.

In a tilt frame - or set of windows, the output of a window can
is the input the next window in a sequence. This is a form of
temporal window-driven event compaction that allows memory be
conserved.

At 1000 events per second, a 1 minute window needs to store 60,000
events per group per second. But 60 1 second windows can be merged
with aggregate functions like `dds` and `hdr` histograms.

Say, each histogram is 1k of memory per group per frame - that is
a cost of 2k bytes per group.

In a streaming system - indefinite aggregation of in memory events is
always a tradeoff against available reosurces, and the relative business
value.

Often multiple windows in a tilt frame can be more effective than a
single very long lived window.

