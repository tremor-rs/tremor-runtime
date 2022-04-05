
```tremor
match event of
  case result = %[ 1, 2 ] => result
  case %[ _ ] => "ignore"
  default => null
end
```

