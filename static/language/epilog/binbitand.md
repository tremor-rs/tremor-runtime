
### Bitwise behaviour, when used with integers

```tremor
  42 & 2 	# 2
  42 & -2 	# 42
  42 & 0 	# 0
  -42 & 2 	# 2
  -42 & -2 	# -42
```

### Logical behaviour, when used with boolean predicates


```tremor
  true & true	# true
  false & true,	# false
```

