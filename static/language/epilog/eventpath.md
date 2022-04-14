
### How do i dereference event data?

The current event is accessed via the `event` keyword.

The event can be any legal tremor value. If it is a record or
an array, then it can be dereferenced via the path language in
the usual way.

```tremor
event.snot; # Event record, field 'snot'
```

```tremor
let badger = event[0]; Event array, first element
```

Events can be mutated and manipulated and used as an output

```tremor
select { "wrapped-event": event } from in into out;
```


