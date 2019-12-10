# The `chash` namespace

The `chash` module contains functions for consistent hashing of values. This can be used to achieve consistent routing over multiple outputs.

## Functions

### chash::jump(key, _slot_count) -> int

Hashes an input `key` (string) and determine its placement in a slot list.

For example It can be used to pick a routing destination using an array of hosts:

```tremor
let hosts = ["host1", "host2", "host3", "host4", "host5"];

{
  "key1": hosts[chash::jump("key1", array::len(hosts))],
  "key1_again": hosts[chash::jump("key1", array::len(hosts))],
  "key2": hosts[chash::jump("key2", array::len(hosts))],
  "key3": hosts[chash::jump("key3", array::len(hosts))],
  "key4": hosts[chash::jump("key4", array::len(hosts))],
}
```

### chash::jump_with_keys(k1, k2, key, _slot_count) -> int

The same as `chash::jump` but uses the integers `k1` and  `k2` to initialise the hashing instead of using default values.

### chash::sorted_serialize(any) -> string

`chash::sorted_serialize` serialised the given data in a sorted and repeatable fashion no matter how data is internally stored. In comparison, the normal serialisation functions do not ensure order for performance reasons. Their behaviour is well suited for encoding data on the wire, but in the context of consistent hashing we need to guarantee that data is always encoded to the same serialisation on a byte level not only on the logical level
