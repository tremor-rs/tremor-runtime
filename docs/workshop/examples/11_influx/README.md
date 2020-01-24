# Transform

This example demonstrates using Tremor as a proxy and aggregator for InfluxDB data. As such it coveres three topics. Ingesting and decoding influx data as the simple part. Then grouping this data and aggregating over it.

The demo starts up a [local Capacitor instance](http://localhost:8888). This allows browsing the data stored in influxdb. When first connecting you'll be asked to specify the database to use, please change the *8Connection URL** to `ihttp://influxdb:8086`. For all other questions just select `Skip` as we do not need to configure those.

Once in capacitor look at the `tremor` database to see the metrics and rollups. Since rollups do roll up over time you might have to wait a few minutes untill aggregated data propagates.

Depending on the performance of the system the demo is run on metrics may be shed due to tremors over load protection.

## Environment

In the [`example.trickle`](etc/tremor/config/example.trickle) we process the data in multiple steps, since this is somewhat more complex then the prior examples we'll discuss each step in the Business Logic section.


## Business Logic

### Grouping

```trickle
select {
    "measurement": event.measurement,
    "tags": event.tags,
    "field": group[2],
    "value": event.fields[group[2]],
    "timestamp": event.timestamp,
}
from in
group by set(event.measurement, event.tags, each(record::keys(event.fields)))
into aggregate
having type::is_number(event.value);
```

This step groups the data for aggregation. This is required since the [Influx Line protocol](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/) allows for multiple values within one message. The grouping step ensures that we do not aggregate `cpu_idle` and `cpu_user` into the same value despite them being in the same result.

In other words we normalise an event like this

```influx
measurement tag1=value1,tag2=value2 field1=42,field2="snot",field3=0.2 123587512345513
```

into the three distinct series it represents, namely:

```influx
measurement tag1=value1,tag2=value2 field1=42 123587512345513
measurement tag1=value1,tag2=value2 field2="snot" 123587512345513
measurement tag1=value1,tag2=value2 field3=0.2 123587512345513
```

The second part that happens in this query is removing non numeric values from our aggregated series since they are not able to be aggregated.

### Aggregation

```trickle
select 
{
    "measurement": event.measurement,
    "tags": patch event.tags of insert "window" => window end,
    "stats": stats::hdr(event.value, [ "0.5", "0.9", "0.99", "0.999" ]),
    "field": event.field,
    "timestamp": win::first(event.timestamp), # we can't use min since it's a float
}
from aggregate[`10secs`, `1min`, ]
group by set(event.measurement, event.tags, event.field)
into normalize;
```

In this section we aggregate the different serieses we created in the previous section.

Most notably are the `stats::hdr` and `win::first` functions which do the aggregation. `stats::hdr` uses a optimized [HDR Histogram](http://hdrhistogram.org/) algorithm to generate the values requested of it. `win::first` gives the timestamp of the first event in the window.

### Normalisation to Influx Line Protocol

```tremor
select {
  "measurement":  event.measurement,
  "tags":  event.tags,
  "fields":  {
    "count_{event.field}":  event.stats.count,
    "min_{event.field}":  event.stats.min,
    "max_{event.field}":  event.stats.max,
    "mean_{event.field}":  event.stats.mean,
    "stdev_{event.field}":  event.stats.stdev,
    "var_{event.field}":  event.stats.var,
    "p50_{event.field}":  event.stats.percentiles["0.5"],
    "p90_{event.field}":  event.stats.percentiles["0.9"],
    "p99_{event.field}":  event.stats.percentiles["0.99"],
    "p99.9_{event.field}":  event.stats.percentiles["0.999"]
  },
  "timestamp": event.timestamp,
}
from normalize
into batch;
```

The last part normalises the data to a format that can be encoded into influx line protocol. And name the fields accordingly. This uses string interpolation for the recortd fields and simle value access for their values.

## Command line testing during logic development

```bash
$ docker-compose up
  ... lots of logs ...
```

Open the [Chronograf](http://localhost:8888) and connect the database.

### Discussion

It is noteworthy that in the aggregation context only `stats::hdr` and `win::first` are being evaluated for events, resulting record and the associated logic is only ever evaluated on emit.

We are using `having` in the goruping step, however this could also be done with a `where` clause on the aggregation step. In this example we choose `having` over were as it is worth discarding events as early as possible. If the requirement were to handle non numeric fields in a different manner routing the output of the grouping step to two different select statements we would have used `where` instead.

### Attention

Using `win::first` over `stats::min` is a debatable choice as we use the timestamp of the first event not the minimal timestamp. Inside of tremor we do not re-order events so those two would result in the same result with `win::first` being cheaper to execute. In addition stats functions are currently implemented to return floating point numbers so `stats::min` could
lead incorrect timestamps we'd rather avoid.
