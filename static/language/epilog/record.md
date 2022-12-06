### How are literal records or map types or key-value collections defined in tremor?

We use `{` .. `}` squigly braces to delimit record -like data.

### Can records be nested?

Record values can be any valid primitive of structural type supported by tremor, including other records.

### Does tremor support typed records?

No. A record in tremor can have element values from any supported primitive or
structural type in tremor.

Validation that a record conforms to a shape, schema or structure can be achieved
through match expressions.

```tremor
use std::type;

let bad = { "list-of-bool": [true, false, "snot"] };
let good = { "list-of-bool": [true, false ], "flag": true };

fn list_of_bool(l) with
  let valid = true;
  for l of
    case (i,e) when type::is_bool(e) and valid == true => let valid = true
    case (i, otherwise) => let valid = false
  end;
  valid # return true if list is all bool, false otherwise
end;

fn is_good_record(r) with
  let valid = false;
  match r of
    case extract=%{ present flag, list-of-bool ~= %[] } 
      when list_of_bool(extract["list-of-bool"]) => 
       let valid = true
    case _ => 
      let valid = false
  end;
  valid
end;

is_good_record(bad); # should fail
is_good_record(good); # should succeed
```

This user defined function can then be used in guard clauses like `when type::is_bool(e) ...` in the
example code.

