
### How do I reference the computed group dimension?

```tremor
use std::record;
define window by_2 from tumbling
with
  size = 2
end;

select {
  "g": group[0], # Extract current group dimension
  "c": aggr::stats::sum(event.c),
}
from in[by_2]
group by set(each(record::keys(event.g))) into out;
```

