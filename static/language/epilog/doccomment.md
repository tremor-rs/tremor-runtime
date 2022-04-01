### Example

Documentation level comments are used throughout the tremor standard library
and used as part of our document generation process.

Here is a modified snippet from the standard library to illustrate

```tremor
## Returns the instance of tremor.
##
## Returns a `string`
intrinsic fn instance() as system::instance;
...
```

This is a builtin function implemented in rust and used in a script as follows:

```tremor
use tremor::system;

system::instance()
```

