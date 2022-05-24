
```tremor
define script boring
script
  drop
end;
create script boring;
select event from in into boring;
select event from boring into out;
```

Drop signals to the tremor runtime that an event is not interesting and can
be dropped without any further handling by the engine. Drop statements in
a script or query result in the processing of the current event halting without
any further action bye the tremor runtime.

The dropped event is discarded by the engine.

