### How do I create new immutable constant variable in tremor?

```tremor
use std::base64;
const SNOT = "snot";
const BADGER = "badger";
const SNOT_BADGER = { "#{snot}": "#{base64::encode(badger)}" };
```

