# General style guide

## unwrap
In runtime code the the use of unwrap & friends is forbidden. Instead errors should be chekced and a proper Result returned or error printed.


### In functions

If a function contains unwraps it often is a good path to make the function return a `Result<()>` even if we do not need the return value.

### For options

In functions returning a Result an Option should be turned into an possible error instead of unwrapping it. This can be done by usin `ok_or_else`
```rust
id.instance_port().unwrap()
// becomes:
id.instance_port().ok_or_else(|| Error::from(format!("missing instance port in {}.", id)))?
```

For unwraps that are known to be Some() (due to prior check) the unwrap can be written more explicitly:

```rust
Ok(self.find(id).unwrap())
// becomes:
if let Some(w) = self.find(id) {
  Ok(&w.artefact)
} else {
  unreachable!()
}
```

### in tests
In tests `expect` can be used instead to give an indication of why something failed other then 'panic':
```rust
let mut runtime = incarnate(config).unwrap();
// becomes:
let mut runtime = incarnate(config).expect("failed to incarnate runtime");
```

In `assert(_eq)!` statments Ok(...) can be shoud used on the right side if we want to test a positive result.
```rust
assert_eq!(
  world.bind_onramp(id.clone()).unwrap(),
  ActivationState::Deactivated,
);
// becomes:
assert_eq!(
  world.bind_onramp(id.clone()),
  Ok(ActivationState::Deactivated),
);
```

