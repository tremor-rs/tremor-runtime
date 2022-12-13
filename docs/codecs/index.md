# Codecs

TL;DR: Codecs are used to describe how to decode data from the wire and encode it back to wire format.

Tremor connects to external systems using [connectors](../connectors). [Connectors](../connectors) use `codecs` to transform the data Tremor receives from connected system participants into a structured value that forms the payload of each and every Tremor event.

Codecs are the means of turning the (mostly binary) data from the wire (e.g. from a TCP connection) into structured values for Tremor events and back into binary wire format.
Each [connector](../connectors) can be configured with a `codec`.

## Usage

If you expect [`JSON`](https://www.json.org/) data from a UDP connection, you need to configure the `json` codec.

Example:

```tremor
define connector udp_example from udp_server
with
    codec = "json",
    config = {
        "url": "localhost:12345"
    }
end;
```


This `udp_example` connector is configured to expect exactly one [`JSON`](https://www.json.org/) document for each UDP Datagram with no leading or tailing bytes

### Codecs and Preprocessors

If you expect line-delimited [`JSON`](https://www.json.org/) instead, with 1 document per line, you need to add a [preprocessor](../preprocessors) that separates the wire data by newline and feeds each line to the codec.

Preprocessors perform various kinds of preprocessing on the wire data, e.g. splitting data by some separator or decompressing data, and multiple can be configured to operate in a chain. The result of this chain, one or multiple chunks of binary data, is passed on to the codec.

This can be done elegantly with the TCP connector instead.

Example:

```tremor
define connector line_delimited_json_via_tcp from tcp_server
with
    preprocessors = [ 
        {
            "name": "separate",
            "config": {
                "separator": "\n"
            }
        } 
    ],
    codec = "json",
    config = {
        "url": "localhost:65535"
    }
end;
```

This `line_delimited_json_via_tcp` connector is now configured to expect 1 [`JSON`](https://www.json.org/) document per line from each accepted TCP connection. Just by adding the [`separate`](../preprocessors/separate.md) Preprocessor.

### Codecs and Postprocessors

If we want to send out line delimited [`JSON`](https://www.json.org/) where each JSON document is base64 encoded, we need to use a [postprocessor](../postprocessors). Postprocessors perform some action on the binary data a codec produces. They can e.g. Split or join the data, compress the data or prefix it with a length-prefix.

Example:

```
define connector my_tcp_client from tcp_client
with
    codec = "json",
    postprocessors = [
        "base64",
        "separate"
    ],
    config = {
        "url": "localhost:9200"
    }
end;
```

This `my_tcp_client` connector is configured to use 2 postprocessors in a chain. First each event is encoded using the [`json`](./json.md) codec, then the encoded binary data is base64-encoded by the [`base64`](../postprocessors/base64.md) postprocessor and finally each resulting chunk of base64 data is split from the next by inserting a `line delimiter` using the [`separate`](../postprocessors/separate.md) postprocessor.


Codecs share similar concepts to [extractors](../extractors), but differ in their application. Codecs are applied to external data as they are ingested by or egressed from a running Tremor process.
Extractors, on the other hand, are used in [scripts](../../language/scripts.md) to extract structured from e.g. strings that are already part of a Tremor event.

## Data Format

Tremor's internal data representation is JSON-like. The supported value types are:

* String- UTF-8 encoded
* Numeric (float, integer)
* Boolean
* Null
* Array
* Record (string keys)
* Binary (raw bytes)