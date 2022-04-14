
### Bitwise behaviour, when used with integers

```tremor
  42 ^ 2	# 40
  42 ^ -2	# -44
  42 ^ 0	# 42
  -42 ^ 2	# -44
  -42 ^ -2	# 40
```

### Logical behaviour, when used with boolean predicates

```tremor
  true ^ true	# false
  true ^ false  # true
```

