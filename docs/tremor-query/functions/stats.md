# The `stats` namespace

The `stats` module contains functions for aggregating statistical measures
of various events.

### stats::count() -> int

Counts the number of events aggregated in the current windowed operation.

```trickle
stats::count() # number of items in the window
```


### stats::min(int|float) -> int|float

Determines the smallest event value in the current windowed operation.

```trickle
stats::min(event.value)
```


### stats::max(int|float) -> int|float

Determines the largest event value in the current windowed operation.

```trickle
stats::max(event.value)
```

#### stats::sum(int|float) -> int|float

Determines the arithmetic sum of event values in the current windowed operation.

```trickle
stats::sum(event.value)
```

#### stats::var(int|float) -> float

Calculates the sample variance of event values in the current windowed operation.

```trickle
stats::var(event.value)
```

#### stats::stdev(int|float) -> float

Calculates the sample standard deviation of event values in the current windowed operation.

```trickle
stats::stdev(event.value)
```


### stats::mean(int|float) -> float

Calculates the stastical mean of the event values in the current windowed operation.

```trickle
stats::mean(event.value)
```

#### stats::hdr(int|float) -> record

Uses a High Dynamic Range ( HDR ) Histogram to calculate all primitive statistics
against the event values sin the current windowed operation. The function additionally interpolates percentiles or quartiles based on a configuration specification passed in as an argument to the aggregater function.

The HDR Histogram trades off memory utilisation for accuracy and is configured
internally to limit accuracy to 2 significant decimal places.

```trickle
stats::hdr(event.value, ["0.5","0.75","0.9","0.99","0.999"])
```

#### stats::dds(int|float) -> record

Uses a Distributed data-stream Sketch ( [DDS (paper)](http://www.vldb.org/pvldb/vol12/p2195-masson.pdf) Histogram to calculate
count, min, max, mean and quartiles with quartile relative-error accurate over the range of points in the histogram. The DDS
histogram trades off accuracy ( to a very low error and guaranteed low relative error ) and unlike HDR histograms does not
need bounds specified.

```trickle
stats::dds(event.value, ["0.5","0.75","0.9","0.99","0.999"])
```
