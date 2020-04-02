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

for (things that deref to) slices becomes:

```rust
if let Some((last_outgoing, other_outgoing)) = outgoing.split_last() {
    // Iterate over all but the last `outgoing`, cloning the `event`
    for (idx, in_port) in other_outgoing {
      self.stack.push((*idx, in_port.clone(), event.clone()))
    }

    // Handle the last `outgoing`, consuming the `event`
    let (idx, in_port) = last_outgoing;
    self.stack.push((*idx, in_port, event))
} else {
    // `outgoing` was empty
}
```

or for iterators over non-slices becomes:

```rust
if outgoing.is_empty() {
    // `outgoing` was empty
} else {
    let len = outgoing.len();

    // Iterate over all but the last `outgoing`, cloning the `event`
    for (idx, in_port) in outgoing.iter().take(len - 1) {
      self.stack.push((*idx, in_port.clone(), event.clone()))
    }

    // Handle the last `outgoing`, consuming the `event`
    let (idx, in_port) = &outgoing[len - 1];
    self.stack.push((*idx, in_port.clone(), event))
}
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
```

With `impl_downcast` we can can pass src as `dyn TremorAggrFn` instead of `dyn Any` locking down 
