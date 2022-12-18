all: operator-docs preprocessor-docs postprocessor-docs codec-docs extractor-docs

operator-docs: 
	scripts/operators.sh

preprocessor-docs: 
	scripts/gen.sh src/preprocessor preprocessors

postprocessor-docs: 
	scripts/gen.sh src/postprocessor postprocessors

codec-docs: 
	scripts/gen.sh src/codec codecs

extractor-docs: 
	scripts/gen.sh tremor-script/src/extractor extractors