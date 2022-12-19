# Preprocessors

Preprocessors operate on the raw data stream and transform it. They are run before data reaches the codec and do not know or care about tremor's internal representation.

Online codecs, preprocessors can be chained to perform multiple operations in succession.