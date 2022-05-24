
```tremor
define pipeline subq
pipeline
  define operator counter from generic::counter;
  create operator counter;
  select event from in into counter;
  select event from counter into out;
end;

create pipeline subq;

select event from in into subq;
select event from subq into out;
```

Uses the builtin counter sequencing operator to numerate a stream.

