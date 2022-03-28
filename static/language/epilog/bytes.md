### Example: How do I encode a TCP packet?

```tremor
# Convert the record into a binary encoded TCP packet
binary::into_bytes(<<
  # Encode source and destination TCP ports, each 16 bits wide
  event.src.port:16,  event.dst.port:16,
  # Encode sequence, 32 bits wide
  event.seq:32,
  # Encode acknowldgement, 32 bits wide
  event.ack:32,
  # Encode TCP conditioning and flags fields
  event.offset:4, event.res:4, event.flags:8, event.win:16,
  # Encode checksum; and urgent bytes from first byte
  event.checksum:16, event.urgent:16,
  # Encode data using the encoded length of another binary literal
  event.data/binary
>>)
```

