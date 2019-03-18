

### Remnve element form a vector:

```rust
self.pipelines.retain(|item| item != to_delete)
```

### Don't clone on the last step of a loop

the loop:
```rust
for (idx, in_port) in outgoing  {
  self.stack.push((*idx, in_port.clone(), event.clone()))
}
```

becomes 
```rust
let len = outgoing.len();
for (idx, in_port) in outgoing.iter().take(len - 1) {
  self.stack.push((*idx, in_port.clone(), event.clone()))
  }
let (idx, in_port) = &outgoing[len - 1];
self.stack.push((*idx, in_port.clone(), event))
```
