### How are literal array or list or vectorc data collections defined in tremor?

We use `[` .. `]` square brackets to delimit list-like data.

### Are multi-dimensional arrays supported?

Multi-dimensional arrays are compositional and can be nested

### Does tremor support typed lists?

No. A list in tremor can have elements from any supported primitive or
structural type.

Validation that a list is for a single type - such as a list of boolean
values can be defined as follows:

```tremor
use std::type;

let bad = [true, false, "snot"];
let good = [true, false ];

fn list_of_bool(l) with
  let valid = true;
  for l of
    case (i,e) when type::is_bool(e) and valid == true => let valid = true
    case (i, otherwise) => let valid = false
  end;
  valid # return true if list is all bool, false otherwise
end;

list_of_bool(bad); # should fail
list_of_bool(good); # should succeed
```

This user defined function can then be used in guard clauses like `when type::is_bool(e) ...` in the
example code.

