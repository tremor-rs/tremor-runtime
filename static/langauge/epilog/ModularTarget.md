### Examples

#### Loading and using a builtin function
```tremor
# Load the base64 utilities
use std::base64;

# Base64 encode the current `event`.
base64::encode(event)
```

#### Loading and using a builtin function with an alias

```tremor
# Load the base64 utilities
use std::base64 as snot;

# Base64 encode the current `event`.
snot::encode(event)
```

