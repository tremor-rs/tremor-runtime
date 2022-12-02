
```tremor
use std::time::nanos;
define connector metronome from metronome
  with
    config = {
      "interval": nanos::from_millis(1)
    }
end;
```

Define user defind connector `metronome` from the builtin `metronome` connector
using a 1 second periodicity interval.

```tremor
define connector exit from exit;
```

Define user dfeind connector `exit` from the builtin `exit` connector
with no arguments specified.

