# The `random` namespace

The `random` module contains functions for generating random values of various types.

The generated values are uniformly distributed over the specified type (and range, where applicable). Useful for doing [percentage drops](../recipes.md#percentage-drops-of-events) of events, for example.

The random number generator under the hood is seeded based on tremor's event ingestion time and thus the output here is deterministic. Should we choose to replay a tremor data dump (something to be added in future tremor versions), we will always get the same value for an event.

## Functions

### random::bool() -> bool

Generates a random boolean.

```tremor
random::bool() # either true or false
```

### random::string(length) -> string

Generates a random string of given `length` with ASCII letters and numbers: a-z, A-Z and 0-9. The argument must be an integer greater than or equal to zero -- otherwise the function errors out.

```tremor
random::string(16) # 16 alphanumeric characters. eg: "QuSFjpW8PBNewRml"
random::string(0)  # ""
```

### random::integer(a?, b?) -> int

Generates a random integer, with the functionality changing based on the number of arguments passed.

#### random::integer(low, high) -> int

Generates random integer between integers `low` and `high` (not including `high`). The first argument must be lower than the second argument -- otherwise the function errors out.

```tremor
random::integer(0, 2)     # either 0 or 1
random::integer(42, 43)   # always 42
random::integer(0, 100)   # one of 0-99
random::integer(-1, 1)    # either -1 or 0
random::integer(-42, -41) # always -42
```

#### random::integer(i) -> int

Generates random integer between zero and integer `i` (not including `i`). The argument must be greater than zero -- otherwise the function errors out.

```tremor
random::integer(100) # one of 0-99. same as random::integer(0, 100)
```

#### random::integer() -> int

Generates a random [integer](../index.md#integer-numerics).

```tremor
random::integer() # an integer
```

### random::float(a?, b?) -> float

Generates a random float, with the functionality changing based on the number of arguments passed.

#### random::float(low, high) -> float

Generates random float between floats `low` and `high` (not including `high`). The first argument must be lower than the second argument -- otherwise the function errors out.

```tremor
random::float(0.0, 100.0) # >= 0.0 and < 100.0
random::float(-1.0, 1.0)  # >= -1.0 and < 1.0
random::float(-3.0, -2.0) # >= -3.0 and < -2.0
```

#### random::float(f) -> float

Generates random float between zero and float `f` (not including `f`). The argument must be greater than zero -- otherwise the function errors out.

```tremor
random::float(100.0) # same as random::float(0.0, 100.0)
```

#### random::float() -> float

Generates a random [float](../index.md#floating-point-numerics) (between `0.0` and `1.0`, not including `1.0`).

```tremor
random::float() # a float (>= 0.0 and < 1.0)
```
