
The builtin `select` operator in queries.

### A simple non-windowed non-grouped select

```tremor
select event from in into out;
```

### A simple non-windowed grouped select

```tremor
select event from in into out group by event.key;
```

### A windowed grouped select operation

```tremor
select event from in[one_sec] by event.key into out;
```

Multiple windows can be configured in lower resolutions
for multi-resolution windowed expressions where lower
resolutions are merged into from higher resolution windows

```tremor
select aggr::stats::hdr(event.count) form in[one_sec, fifteen_sec, one_min, one_hour] into out;
```

