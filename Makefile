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
tremor-image:
	docker build -f docker/tremor-runtime.dockerfile . -t tremor-runtime
loadgen-image: tremor-image
	docker build -f demo/loadgen.dockerfile . -t loadgen
demo-images: loadgen-image

demo-containers: demo-images
	@echo "**************************************************************"
	@echo "demo-containers is deprecated, please use demo-images instead."
	@echo "**************************************************************"

demo-run:
	-docker-compose -f demo/demo.yaml rm -fsv
	-docker-compose -f demo/demo.yaml up
	-docker-compose -f demo/demo.yaml rm -fsv

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

rpm: force
	docker build . -f rpm/Dockerfile -t tremor-rpm-build
	-mkdir rpm/out
	docker run --name tremor-rpm-build-copy tremor-rpm-build /bin/true
	docker cp tremor-rpm-build-copy:/root/rpmbuild/RPMS/x86_64/tremor-1.0.0-1.x86_64.rpm rpm/out
	docker rm -f tremor-rpm-build-copy

force:
	true
