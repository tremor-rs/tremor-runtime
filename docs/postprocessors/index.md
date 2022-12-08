# Postprocessors

Postprocessors operate on the raw data stream and transform it. They are run after data reaches the codec and do not know or care about tremor's internal representation.

Online codecs and postprocessors can be chained to perform multiple operations in succession.

## Configuration

Postprocessors can be configured by specifying them by name or by name and config. Check out the following example that exhibits both forms:

```
define connector my_file from file
with
    codec = "json",
    postprocessors = [
        "base64",
        {
            "name": "separate",
            "config": {
                "separator": "|"
            }
        }
    ],
    config = {
        "path": "out.json",
        "mode": "truncate"
    }
end;
```