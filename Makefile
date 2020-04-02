APP=tremor-runtime
DOCKER_VSN=$(shell grep 'ARG tag' docker/tremor-runtime.dockerfile | sed 's/.*=//')
CARGO_VSN=$(shell grep '^version' Cargo.toml | sed -e 's/.*=[^"]*"//' -e 's/"$$//')
VSN=$(DOCKER_VSN)
YEAR=2018-2020

help:
	@echo "This makefile wraps the tasks:"
	@echo "  image - builds the image"
	@echo "  demo - runs a simple demo"
	@echo "  bench - runs benchmarks"
	@echo "  it - runs integration tests"

image:
	docker-compose build

demo: image
	-docker-compose -f demo/demo.yaml rm -fsv
	-docker-compose -f demo/demo.yaml up
	-docker-compose -f demo/demo.yaml rm -fsv

it:
	integration_testing/runner

stdlib-doc:
	-rm -rf docs;
	for file in `find tremor-script/lib -type f`; do \
	dir=`dirname $$file | sed -e 's;^tremor-script/lib;docs;'`;\
	base=`basename $$file`;\
	output_file=`echo $$base | sed -e 's/.tremor/.md/'`;\
	echo "$$file -> $$dir/$$output_file"; \
	[ -d $$dir ] || mkdir -p $$dir;\
	TREMOR_PATH=tremor-script/lib cargo run -p tremor-script -- $$file --docs $${output_file%.*}  > $$dir/$$output_file;\
	done

bench: force
	cargo build --release -p tremor-server
	./bench/all.sh | tee bench-`date +%s`.txt

force:
	true

chk_copyright:
	@./.github/checks/copyright.sh

chk:
	@./.github/checks/safety.sh -a

dep-list:
	@cargo tree --all | sed -e 's/[^a-z]*\([a-z]\)/\1/' | sort -u
