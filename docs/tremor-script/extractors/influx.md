# Influx

Influx extrector matches data from the string that uses the [Influx Line Protocol](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/). It will fail if the input isn't a valid string.

## Predicate

When used as a predicate with `~`, the predicate will pass if the target conforms to the influx line protocol.

## Extraction

The extractor will return a record with the measurement, fields, tags and the timestamp extracted from the input.

Example:

```tremor
match { "meta" :  "wea\\ ther,location=us-midwest temperature=82 1465839830100400200" } of
  case rp = %{ meta ~= influx||} => rp
  default => "no match"
end;
```

This will return:

```bash
"meta": {
          "measurement": "wea ther",
           "tags": {
               "location": "us-midwest"
             },
             "fields": {
              "temperature": 82.0
            },
            "timestamp": 1465839830100400200
        }
```
