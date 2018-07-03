APP=traffic-shaping-tool

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

goss: build
	GOSS_FILES_PATH=docker/ dgoss run --name "example-dgoss-test" --rm "wayfair/data-engineering/$(APP)"

