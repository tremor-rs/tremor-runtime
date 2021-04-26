APP=tremor-runtime
DOCKER_VSN=$(shell grep 'ARG tag' docker/tremor-runtime.dockerfile | sed 's/.*=//')
CARGO_VSN=$(shell grep '^version' Cargo.toml | sed -e 's/.*=[^"]*"//' -e 's/"$$//')
VSN=$(DOCKER_VSN)
YEAR=2018-2021

RELEASE_TARGETS := \
	x86_64-unknown-linux-gnu \
	# x86_64-unknown-linux-musl \
	# x86_64-alpine-linux-musl \
	# fails on snmalloc builds currently so disabled
	# TODO if we fix this, we don't need the alpine specifc target above for musl builds

# Please keep the RELEASE_FORMATS_* vars here aligned with RELEASE_TARGETS.
#
# For x86_64, packaging built on top of glibc based binaries is our primary
# means of distribution right now. Using musl targets here would give us
# fully static binaries (for easier distribution), but we are seeing up to
# 25% slowdown for some of our benchmarks with musl builds. So we stick with
# gnu builds for now.
#
# The deb packaging is disabled here for now, since we currently make the binary
# for x86_64-unknown-linux-gnu target in a centos7 docker container and binary
# produced there does not easily port to debian (due to the dynamic ssl linking).
# TODO resolve how we want to handle debian package building: either statically
# link openssl from the centos7 image, or separate out binary building for debian
# packaging using debian image itself (might also need to have separate build for
# archive packaging too).
#RELEASE_FORMATS_x86_64-unknown-linux-gnu := archive,deb,rpm
RELEASE_FORMATS_x86_64-unknown-linux-gnu := archive,rpm,deb
RELEASE_FORMATS_x86_64-alpine-linux-musl := archive
RELEASE_FORMATS_x86_64-unknown-linux-musl := archive

help:
	@echo "This makefile wraps the tasks:"
	@echo "  image                - build the docker image"
	@echo "  demo                 - run a simple demo"
	@echo "  it                   - run integration tests"
	@echo "  bench                - run benchmarks"
	@echo "  builder-image-TARGET - build the (builder) docker image used for building against the specified TARGET"
	@echo "  builder-images       - build all the (builder) docker images used for building against the release targets"
	@echo "  build-TARGET         - build for the specified TARGET"
	@echo "  builds               - build for all the release targets"
	@echo "  archive-TARGET       - package release archive for the specified TARGET"
	@echo "  archives             - package release archive for all the release targets"
	@echo "  package-TARGET       - package (across applicable formats) for the specified TARGET"
	@echo "  packages             - package (across applicable formats) for all the release targets"

###############################################################################

image:
	$(eval BRANCH_HASH='$(shell git rev-parse --abbrev-ref HEAD):$(shell git rev-parse HEAD)')
	docker-compose build --build-arg BRANCH_HASH=$(BRANCH_HASH)

demo: image
	-docker-compose -f demo/demo.yaml rm -fsv
	-docker-compose -f demo/demo.yaml up
	-docker-compose -f demo/demo.yaml rm -fsv

it:
	cargo install --path tremor-cli
	cd tremor-cli && TREMOR_PATH=../tremor-script/lib tremor test integration tests -i integration -e ws

bench: force
	cargo build --release -p tremor
	./bench/all.sh | tee bench-`date +%s`.txt

force:
	true

###############################################################################

stdlib-doc:
	-rm -rf docs
	-mkdir docs
	-TREMOR_PATH=./tremor-script/lib cargo run -p tremor-cli -- doc tremor-script/lib docs

chk_copyright:
	@./.github/checks/copyright.sh

chk:
	@./.github/checks/safety.sh -a

dep-list:
	@cargo tree --all | sed -e 's/[^a-z]*\([a-z]\)/\1/' | sort -u

clippy:
	touch */src/lib.rs && \
	touch src/lib.rs && \
	touch */src/main.rs && \
	cargo clippy --all

###############################################################################

# eg: builder-image-x86_64-unknown-linux-gnu
builder-image-%:
	@echo ""
	./packaging/builder-images/build_image.sh $*

builder-images:
	make $(foreach target,$(RELEASE_TARGETS),builder-image-$(target))

# eg: build-x86_64-unknown-linux-gnu
build-%:
	@echo ""
	./packaging/cross_build.sh $*

builds:
	make $(foreach target,$(RELEASE_TARGETS),build-$(target))

# eg: archive-x86_64-unknown-linux-gnu
archive-%: build-%
	@echo ""
	./packaging/run.sh -f archive $*

archives:
	make $(foreach target,$(RELEASE_TARGETS),archive-$(target))

# eg: package-x86_64-unknown-linux-gnu
package-%: build-%
	@echo ""

	@# ensure that we have RELEASE_FORMATS_* var defined here for the provided target
	@echo "Packaging for target: $*"
	@RELEASE_FORMATS=$(RELEASE_FORMATS_$*); \
		if [ -z "$${RELEASE_FORMATS}" ]; then \
			echo "Error: Variable RELEASE_FORMATS_$* not set in the Makefile"; \
			exit 1; \
		fi

	@# package applicable formats for the given release target
	./packaging/run.sh -f $(RELEASE_FORMATS_$*) $*

packages:
	make $(foreach target,$(RELEASE_TARGETS),package-$(target))

###############################################################################
