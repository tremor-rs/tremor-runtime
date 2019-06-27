# CIDR

Classless Inter-Domain Routing ( CIDR) is a method of allocating IP addresses
and IP routing paths. CIDR notation is a compact representation of an IP address and
its associated routing prefix.

The notation is constructed from a possibly incomplete IP address, followed by a slash (`/`) character, and then a decimal number. 

The number is the count of leading *1* bits in the subnet mask demarcating routing boundaries for the range of addresses being routed over.

Larger values here indicate smaller networks. The maximum size of the network is given by the number of addresses that are possible with the remaining, least-significant bits below the prefix.

## Predicate

When used as a predicate test with `~`, and no arguments are provided, then any valid IP
address will pass the predicate test.

When used as a predicate test with `~`, with one or many command delimited CIDR forms, then any valid IP address must be within the specified set of CIDR patterns for the predicate
to pass.

Pattern forms may be based on IPv4 or IPv6.

## Extraction

When used as an extraction operation with `~=` the predicate test must pass for
successful extraction.  If the predicate test succeeds, then the the prefix and
network mask for each CIDR is provided as a key/value record with the original
pattern form as key.


## Examples

```tremor
match { "meta": "192.168.1.1"} of
  case rp = %{ meta ~= cidr|| } => rp
  default => "no match"
end;

```

This will output:

```bash
"meta":{
  "prefix":[ 192, 168, 1, 1 ],
  "mask": [ 255, 255, 255, 255 ]
 }
```

Cidr also supports filtering the IP if it is within the CIDR specified. This errors out if the IP Address specified isn't in the range of the CIDR. Otherwise, it will return the prefix & mask as above.

```tremor
match { "meta": "10.22.0.254" } of
  case rp = %{ meta ~= cidr|10.22.0.0/24| } => rp
  default => "no match"
end;
```

This will output:

```bash
"meta":{
  "prefix":[ 10, 22, 0, 254 ],
  "mask": [ 255, 255, 255, 255 ]
 }
```

The extractor also supports multiple comma-separated ranges. This will return the prefix and mask if it belongs to any one of the CIDRs specified:

```tremor
match { "meta": "10.22.0.254" } of
  case rp = %{ meta ~= cidr|10.22.0.0/24, 10.21.1.0/24| } => rp
  default => "no match"
end;
```

This will output:

```bash
"meta":{
  "prefix":[ 10, 22, 0, 254 ],
  "mask": [ 255, 255, 255, 255 ]
 }
```

In case it doesn't belong to the CIDR:

```tremor
match { "meta": "19.22.0.254" } of
  case rp = %{ meta ~= cidr|10.22.0.0/24, 10.21.1.0/24| } => rp
  default => "no match"
end;
## Output: "no match"
```
