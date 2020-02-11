APP=tremor-runtime
DOCKER_VSN=$(shell grep 'ARG tag' docker/tremor-runtime.dockerfile | sed 's/.*=//')
CARGO_VSN=$(shell grep '^version' Cargo.toml | sed -e 's/.*=[^"]*"//' -e 's/"$$//')
VSN=$(DOCKER_VSN)
YEAR=2018-2019

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

bench: force
	cargo build --release --features bench
	./bench/all.sh | tee bench-`date +%s`.txt

force:
	true

chk_copyright:
	@for f in `find . -name '*.rs' | grep -v '/target'`; do cat $$f | grep 'Copyright 2018-2020, Wayfair GmbH' > /dev/null || (echo "##[error] No copyright in $$f") done

chk_copyright_ci:
	@for f in `find . -name '*.rs' | grep -v '/target'`; do cat $$f | grep 'Copyright 2018-2020, Wayfair GmbH' > /dev/null || exit 1; done

chk_unwrap:
	@./checks/safety.sh -u

chk_unwrap_ci:
	@./checks/safety.sh -u

chk_panic:
	@./checks/safety.sh -p

chk_panic_ci:
	@./checks/safety.sh -p

dep-list:
	@cargo tree --all | sed -e 's/[^a-z]*\([a-z]\)/\1/' | sort -u
