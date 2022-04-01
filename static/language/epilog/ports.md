A set of `ports` exposed in pipeline definitions in their `from` and `into` clauses

```tremor
define pipeline example
  from in, out, err, ctrl
  into in, out, err, ctrl
pipeline
  # A pipeline query implementation
  ...
end
```

The `from` and `into` ports do not need to be the same.

Tremor's compiler and runtime can use these definitions to validate deployments
are correct, or discover deployments that are invalid. It is an error to send
data to or receive data from a pipeline port that is not specified.

