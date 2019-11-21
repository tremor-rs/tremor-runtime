# Preprocessors

Preprocessors operate on the raw data stream and transform it. They are executed before data reaches the codec and do not know or care about tremors internal representation.

Online codecs, preprocessors can be chained to perform multiple operations in succession.

## Supported Preprocessors

### lines

Splits the input into lines (character 13 `\n` as line separator)

### lines-null

Splits the input into lines (null byte `\0` as line separator)

### lines-pipe

Splits the input into lines (pipe `|` as line separator)

### base64

Decodes base64 encoded data to the raw bytes.

### decompress

Decompresses a data stream, it is assumed that each message reaching the decompressor is a complete compressed entity.

The compression algorithm is detected automatically from the supported formats, if it can't be detected the  assumption is that the data was decompressed and will be send on. Failure then can be transparently handled in the codec.

Supported formats:

* gzip
* zlib
* xz
* snappy
* lz4

### gzip

Decompress GZ compressed payload

### zlib

Decompress Zlib ( deflate ) compressed payload

### xz

Decompress Xz2 ( 7z ) compressed payload

### snappy

Decompress framed snappy compressed payload ( does not support raw snappy )

### lz4

Decompress Lz4 compressed payload

### gelf-chunking

Reassembles messages that were split apart using the [GELF chunking protocol](https://docs.graylog.org/en/3.0/pages/gelf.html#gelf-via-udp). The message content is decompressed after reassembly so no additional decompression is needed.

### remove-empty

Removes empty messages (aka zero len).

### length-prefixerd

Seperates a continous stream of data based on length prefixing. The lenght for each package in a stream is based on the first 64 bit decoded as a unsigned big endian value.