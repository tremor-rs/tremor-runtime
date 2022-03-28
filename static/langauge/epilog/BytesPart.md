### Form

The part may take the following general form

```ebnf
SimpleExprImut  ':'  'int'  '/' Ident 
```

Where:
* The `SimpleExprImut can be a literal or identifier to the data being encoded.
* A optional size in bits, or defaulted based on the data being encoded.
* An optional encoding hint as an identifier

### Size constraints

The size must be zero or greater, up to and including but no larger than 64 bits.

### Encoding Hints

|Ident|Description|
|---|---|
|`binary`|Encoded in binary, using network ( big ) endian|
|`big-unsigned-integer`|Unsigned integer encoding, big endian|
|`little-unsigned-integer`|Unsigned integer encoding, little endian|
|`big-signed-integer`|Signed integer encoding, big endian|
|`little-signed-integer`|Signed integer encoding, little endian|


