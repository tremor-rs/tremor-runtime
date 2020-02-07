Notes:



Time spent in the onramp in 'crossbeam_channel_sender'  is spent waiting for the pipeline.



Heuristics:

About 10% are spent in 'non hot functions', so if we approach 90% of weight on a single function it means contention on this.

Onramp: In the time in 'Crossbeam channel Sender send' approaches zero (aka ~10%) we are limited by the onramp

Pipeline: If the Weight (not self weight) of 'ExecutableGraph::run' approaches 100% the we are limited by the pipeline