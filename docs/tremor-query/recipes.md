# Periodic Synthetic Metrics Events

Periodically, produce basic statistics and percentiles / quartiles from a stream of ingested events, for a particular value in the inbound event stream.

```trickle
# Every 10 seconds
create tumbling window `10secs`
 with
   interval = datetime::with_seconds(10),
end;

# Aggregate events producing statistics into a temporary stream
select {
    "measurement": event.measurement,
    "tags": patch event.tags of insert "window" => "10s" end,
    "stats": stats::hdr(event.fields[group[2]], [ "0.42", "0.5", "0.9", "0.99", "0.999" ]),
    "class": group[2]
}
from in[`10secs`]
group by set(event.measurement, event.tags, each(record::keys(event.fields)))
into normalize
having event.stats.count > 100; # discard if not enough sample data for group

# create a temporary stream to normalize results
create stream normalize;

# normalize output record to match requirements downstream ( influx )
select {
  "measurement":  event.measurement,
  "tags":  event.tags,
  "fields":  {
    "count_{event.class}":  event.stats.count,
    "min_{event.class}":  event.stats.min,
    "max_{event.class}":  event.stats.max,
    "mean_{event.class}":  event.stats.mean,
    "stdev_{event.class}":  event.stats.stdev,
    "var_{event.class}":  event.stats.var,
    "p42_{event.class}":  event.stats.percentiles["0.42"],
    "p50_{event.class}":  event.stats.percentiles["0.5"],
    "p90_{event.class}":  event.stats.percentiles["0.9"],
    "p99_{event.class}":  event.stats.percentiles["0.99"],
    "p99.9_{event.class}":  event.stats.percentiles["0.999"]
  }
}
from normalize
into out;
```
