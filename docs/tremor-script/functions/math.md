# The `math` namespace

The `math` module contains functions for common mathematical operations.

## Functions

### math::floor(n) -> int

Returns the largest integer value less than or equal to `n`.

```tremor
math::floor(42.9) # 42
```

### math::ceil(n) -> int

Returns the smallest integer value greater than or equal to `n`.

```tremor
math::ceil(41.1) # 42
```

### math::round(n) -> int

Returns the integer nearest to `n`.

```tremor
math::round(41.4) # 41
math::round(41.5) # 42
```

### math::trunc(n) -> int

Returns the integer part of `n`.

```tremor
math::trunc(42.9) # 42
```

### math::max(n1, n2) -> number

Returns the maximum of two numbers.

```tremor
math::max(41, 42) # 42
```

### math::min(n1, n2) -> number

Returns the minimum of two numbers.

```tremor
math::min(42, 43) # 42
```
