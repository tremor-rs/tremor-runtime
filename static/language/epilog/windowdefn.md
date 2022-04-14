
Operations supporting windowed aggregate functions in tremor such as the `select`
statement can window incoming streams in the `from` clause:

```tremor
select aggr::count(event) from in[one_second, ten_second]
...
```

Here, we stream events from the `in` stream into a `one_second` window.
Every second, we stream the aggregate result from the one second window
into the `ten_second` window.

So, even if we have 1 million events per second, the `one_second` and `ten_second`
windows will convert the event firehose into a `trickle`. Fun fact: this pun is where the
query language got its name from.

