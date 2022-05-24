
```tremor
define script route
script
  emit => "not_out"
end;

create script route;
select event from in into route;
select event from route/not_out into out;
```

Emit signals to the tremor runtime that an event has been processed fully and
processing can stop at the point emit is invoked and a synthetic value returned
without any further processing.

The emitted event is forwarded by the engine.

