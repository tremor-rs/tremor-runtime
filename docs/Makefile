all: operator-docs preprocessor-docs postprocessor-docs codec-docs extractor-docs connector-docs

operator-docs: 
	scripts/operators.sh

preprocessor-docs: 
	scripts/gen.sh tremor-interceptor/src/preprocessor preprocessors

postprocessor-docs: 
	scripts/gen.sh tremor-interceptor/src/postprocessor postprocessors

codec-docs: 
	scripts/gen.sh tremor-codec/src/codec codecs

extractor-docs: 
	scripts/gen.sh tremor-script/src/extractor extractors

connector-docs: 
	scripts/gen.sh tremor-connectors/src/impls connectors
	scripts/gen.sh tremor-connectors-gcp/src connectors
	scripts/gen.sh tremor-connectors-aws/src connectors
	scripts/gen.sh tremor-connectors-otel/src connectors
	scripts/gen.sh tremor-connectors-azure/src connectors
