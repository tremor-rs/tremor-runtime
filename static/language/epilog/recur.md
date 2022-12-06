
```tremor
fn fib_(a, b, n) of
  case (a, b, n) when n > 0 => recur(b, a + b, n - 1)
  case _ => a
end;

fn fib(n) with
  fib_(0, 1, n)
end;
```

Tremor's functional programming langauge supports tail recursion via the
`recur` keyword. Tail recursion in tremor is limited to a fixed stack
depth - infinite recursion is not permissible.

