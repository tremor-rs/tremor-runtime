
```tremor
define flow test
flow
  use std::time::nanos;
  define connector metronome from metronome
  with
    config = {
      "interval": nanos::from_millis(1)
    }
  end;
  define connector exit from exit;
  define pipeline identity
  pipeline
    select event from in into out;
  end;
  create connector metronome;
  create connector exit;
  create pipeline identity;
  connect /connector/metronome to /pipeline/identity;
  connect /pipeline/identity to /connector/exit;
end;

# The `deploy` statements commands tremor to instanciate the flow `test` - the flow in turn
# will result in a metronome, exit connector, pipeline to be started and interconnected as
# per the `test` definition above
deploy flow test
```

