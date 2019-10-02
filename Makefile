APP=tremor-runtime
DOCKER_VSN=$(shell grep 'ARG tag' docker/tremor-runtime.dockerfile | sed 's/.*=//')
CARGO_VSN=$(shell grep '^version' Cargo.toml | sed -e 's/.*=[^"]*"//' -e 's/"$$//')
VSN=$(DOCKER_VSN)
YEAR=2018-2019

help:
	@echo "This makefile wraps the tasks:"
	@echo "  image - builds the image"
	@echo "  demo - runs a simple demo"
	@echo "  clippy-install - install nightly and clippy"
	@echo "  clippy - runs clippy"
	@echo "  bench - runs benchmarks"
	@echo "  it - runs integration tests"
	@echo "  doc - creates and opens documentation"

image:
	docker-compose build

demo: image
	-docker-compose -f demo/demo.yaml rm -fsv
	-docker-compose -f demo/demo.yaml up
	-docker-compose -f demo/demo.yaml rm -fsv

it:
	integration_testing/runner

doc: force
	cargo doc --open --no-deps
	rm -r doc
	cp -r target/doc .

bench: force
	cargo build --release --features bench
	./bench/all.sh | tee bench-`date +%s`.txt

force:
	true

chk_copyright:
	@for f in `find . -name '*.rs' | grep -v '/target'`; do cat $$f | grep 'Copyright 2018-2019, Wayfair GmbH' > /dev/null || (echo "##[error] No copyright in $$f") done

chk_copyright_ci:
	@for f in `find . -name '*.rs' | grep -v '/target'`; do cat $$f | grep 'Copyright 2018-2019, Wayfair GmbH' > /dev/null || exit 1; done

chk_unwrap:
	@for f in `find . -name '*.rs' | grep -v '/target' | grep -v 'property_testing' | grep -v 'depricated'`; do grep 'unwrap()' $$f > /dev/null && (echo "##[error] unwrap found in $$f") done

chk_unwrap_ci:
	@for f in `find . -name '*.rs' | grep -v '/target' | grep -v 'property_testing' | grep -v 'depricated'`; do cat $$f | grep 'unwrap()' > /dev/null && exit 1; done; exit 0

chk_panic:
	@for f in `find . -name '*.rs' | grep -v '/target' | grep -v 'property_testing' | grep -v 'depricated'`; do grep 'panic!(' $$f > /dev/null && (echo "##[error] panic found in $$f") done

chk_panic_ci:
	@for f in `find . -name '*.rs' | grep -v '/target' | grep -v 'property_testing' | grep -v 'depricated'`; do cat $$f | grep 'panic!(' > /dev/null && exit 1; done; exit 0

docserve:
	mkdocs serve

tarpaulin:
	@docker build . -f Dockerfile.tarpaulin -t tremor-tarpaulin
	@docker run --privileged --mount type=bind,source="$$(pwd)",target=/code -t tremor-tarpaulin
	@echo "To view run: pycobertura show --format html --output coverage.html cobertura.xml && open coverage.html"
	@echo "  pycobertura can be installed via pip3 install pycobertura"
