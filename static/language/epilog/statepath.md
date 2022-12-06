
### How do i reference state in tremor?

Tremor programs can be stateful in many ways - such as through the state managed
by operators and windowed operations by the runtime on behalf of the user provided
program.

The `state` keyword allows an arbitrary value controlled by a users program to be
maintained and managed by the user program.

```tremor
let my_state = state;
```

State can be written through via a `let` operation

```tremor
let state = match state of
  case null => { "count": 1 }
  case _ => { "count"": state.count + 1 }
end;
```

