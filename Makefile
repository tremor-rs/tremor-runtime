APP=tremor-runtime
DOCKER_VSN=$(shell grep 'ARG tag' docker/tremor-runtime.dockerfile | sed 's/.*=//')
CARGO_VSN=$(shell grep '^version' Cargo.toml | sed -e 's/.*=[^"]*"//' -e 's/"$$//')
VSN=$(DOCKER_VSN)

help:
	@echo "This docker files wraps the tasks:"
	@echo "  build - builds the container using two stage builds"
	@echo "  all - meta task to execute lint, build and goss"

all: lint build goss

loadgen-image: tremor-image
	docker build -f demo/loadgen.dockerfile . -t loadgen
demo-images: loadgen-image

tremor-image:
	docker build . -t tremor-runtime

demo-run:
	-docker-compose -f demo/demo.yaml rm -fsv
	-docker-compose -f demo/demo.yaml up
	-docker-compose -f demo/demo.yaml rm -fsv

demo-influx-run:
	-docker-compose -f demo/influx.yaml rm -fsv
	-docker-compose -f demo/influx.yaml up
	-docker-compose -f demo/influx.yaml rm -fsv

demo-mssql-run:
	-docker-compose -f demo/mssql.yaml rm -fsv
	-docker-compose -f demo/mssql.yaml up
	-docker-compose -f demo/mssql.yaml rm -fsv

demo-all-run:
	-docker-compose -f demo/all.yaml rm -fsv
	-docker-compose -f demo/all.yaml up
	-docker-compose -f demo/all.yaml rm -fsv

demo-all-bootstrap:
	cd demo && ./grafana-bootstrap.sh
	telegraf -config demo/telegraf.conf

clippy-install:
	rustup update
	rustup install nightly
	rustup component add clippy-preview --toolchain=nightly

clippy:
	CARGO_TARGET_DIR=target.clippy cargo +nightly clippy

it:
	integration_testing/runner

publish-bench:
	cargo build --release --examples
	for f in bench/*.sh; do $$f; done

force:
	true

doc:
	cargo doc --open --no-deps
