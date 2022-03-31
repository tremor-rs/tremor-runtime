### Example

An effector is an `=>` arrow followed by `Block` sequence.

In many of the structural forms such as `match`, `for` or `patch` and effector is
a sequence of logic that is executed when certain conditions occur. The final statement
in an effector is the result of the sequence of executions.

As an example, here is a `for` compresension that enumerates a list
and computes the stringified representation of the elements of the list.

The `for` expression collects each iterations result from the effector's `block`
statement and aggregates them into a list.

```tremor
for [1, 2, 3, 4, 5] of
  case (i, e) => "#{e}";
end;

```
