# The `win` namespace

The `win` module contains functions for aggregating over the current active window in a window of events. The functions can also be used in tilt frames where events emitting from a window are chained across multiple window frames in sequence.

## Functions

### win::first() -> event

Capture and return the first event that hits a window upon/after opening.

```trickle
win::first() # first event in a window
```

### win::last() -> event

Capture and return the last event that hits a window upon/after opening.

```trickle
win::last()
```

### win::collect_flattened() -> [event]

Captures all events in a window into an array of events.

In the case of tilt frames,  flattens out any tilt frame sub-arrays

```trickle
win::collect_flattened()
```

#### win::collect_nested() -> [[event]]|[event]

Captures all events in a window into an array of events.

In the case of tilt frames, each frame is preserved as a nested array of arrays. For a tilt frame of 3 windows, the inner-most leaf array contains events, and higher levels are arrays of arrays.

```trickle
win::collect_nested()
```
