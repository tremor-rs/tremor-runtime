### Example

The following basic forms are permissible

```tremor
for event.object of case (k, v) => v end;
for event.list of case (i, e) => e end;
match event of case %{} => "record" case _ => "not a record" end;
let list = event.list;
drop;
"any literal or basic expression";
1 + 2 * 3;
emit {"snot": "badger" }
# ...
```

