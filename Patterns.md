### Remove element from a vector:

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

becomes:

```rust
let len = outgoing.len();
for (idx, in_port) in outgoing.iter().take(len - 1) {
  self.stack.push((*idx, in_port.clone(), event.clone()))
  }
let (idx, in_port) = &outgoing[len - 1];
self.stack.push((*idx, in_port.clone(), event))
```

### Rewrite entry to not require a clone when the value exists

```rust
*self.metrics[*idx].outputs.entry(in_port.clone()).or_insert(0) += 1;
```

becomes where the clone is limited to only happen when the element doesn't exist.

```rust
if let Some(count) = self.metrics[*idx].outputs.get_mut(in_port) {
  *count += 1;
} else {
  self.metrics[*idx].outputs.insert(in_port.clone(), 1);
}
```

### Getting rid of dyn any

https://docs.rs/downcast-rs/1.1.0/downcast_rs/ allows downcasting on traits. An example can be found in `TremorAggrFn`.

```rust
pub trait TremorAggrFn: DowncastSync + Sync + Send {
  //...
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()>;
  //...
}
impl_downcast!(sync TremorAggrFn);

