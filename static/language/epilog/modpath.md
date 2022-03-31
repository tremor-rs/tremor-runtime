
### How do i reference something from the standard library?

The standard library contains reusable constants, functions
and other definitions that can be used in scripts via the
`Use` and `ModPath` rules.

For example, if you have a file called `foo.tremor` in a `src`
folder you can append this to your `TREMOR_PATH` environment
variable

```bash
export TREMOR_PATH=/path/to/src
```

Assuming `foo.tremor` contains the following code:

```tremor
fn meaning_of_life() of
  42
end;
```

We can use this in another script as follows:

```tremor
use foo;

let meaning = foo::meaning_of_life();
```

