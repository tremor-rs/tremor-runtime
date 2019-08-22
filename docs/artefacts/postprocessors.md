# Postprocessors

Postprocessors operate on the raw data stream and transform it. They are executed after data reaches the codec and do not know or care about tremors internal representation.

Online codecs, postprocessors can be chained to perform multiple operations in succession.

## Supported Postprocessors

### lines

Delimits the output ( events ) into lines ( by '\n' )

### base64

Encodes raw data into base64 encoded bytes.

### compression

Compresses event data.

Unlike decompression processors, the compression algorithm must be selected. The following
compression post-processors are supported. Each format can be configured as a postprocessor.

Supported formats:

* gzip      - GZip compression
* zlib      - ZLib compression
* xz        - Xz2 level 9 compression
* snappy    - Snappy compression
* lz4       - Lz level 4 compression
