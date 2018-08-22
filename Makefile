APP=tremor-runtime

help:
	@echo "This docker files wraps the tasks:"
	@echo "  build - builds the container using two stage builds"
	@echo "  lint - lints the docker file"
	@echo "  goss - uses the goss rules in docker/goss.yaml to test the image (will also run build)"
	@echo "  all - meta task to execute lint, build and goss"

all: lint build goss

build:
	docker-compose build $(APP)

lint:
	docker run -it --rm --privileged -v $$PWD:/root/ \
		artifactory.service.bo1.csnzoo.com/external/projectatomic/dockerfile-lint \
		dockerfile_lint -p -f docker/$(APP).dockerfile -r default_rules.yaml

goss:
	GOSS_FILES_PATH=docker/ dgoss run --name "$(APP)-dgoss-test" --rm "wayfair/data-engineering/$(APP)"

demo-containers:
	docker build -f docker/tremor-runtime.dockerfile . -t tremor-runtime
	docker build -f demo/loadgen.dockerfile . -t loadgen

demo-run:
	-docker-compose -f demo/demo.yaml rm -fsv
	-docker-compose -f demo/demo.yaml up
	-docker-compose -f demo/demo.yaml rm -fsv

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
