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
	docker-compose build

demo: image
	-docker-compose -f demo/demo.yaml rm -fsv
	-docker-compose -f demo/demo.yaml up
	-docker-compose -f demo/demo.yaml rm -fsv

it-clean:
	-find tremor-cli/tests -name '*.log' | xargs rm
it: it-clean
	cargo run -p tremor-cli -- test integration tremor-cli/tests

bench: force
	cargo build --release -p tremor-cli
	./bench/all.sh | tee bench-`date +%s`.txt

force:
	true

###############################################################################

docs: library-doc lalrpop-doc
	-cp tremor-script/docs/library/overview.md docs/library

library-doc:
	-rm -rf docs
	-mkdir -p docs/library
	-TREMOR_PATH=./tremor-script/lib cargo run -p tremor-cli -- doc tremor-script/lib docs/library

lalrpop-docgen:
	-git clone https://github.com/licenser/lalrpop lalrpop-docgen
	cd lalrpop-docgen && git checkout docgen

lalrpop-doc: lalrpop-docgen
	-mkdir docs/language
	cd lalrpop-docgen && cargo build --all
	lalrpop-docgen/target/debug/lalrpop-docgen \
	  -rp "./" \
	  -mp static/language/prolog/ \
          -me static/language/epilog/ \
          -gc "ModuleFile,Deploy,Query,Script" \
          --out-dir docs/language \
          tremor-script/src/grammar.lalrpop
	if test -f docs/language/grammar.md; then  mv docs/language/grammar.md docs/language/EBNF.md; fi
	if test -f docs/language/modulefile.md; then mv docs/language/modulefile.md docs/language/module_system.md; fi

lint-lalrpop-doc: lalrpop-docgen
	-mkdir docs/language
	cd lalrpop-docgen && cargo build --all
	lalrpop-docgen/target/debug/lalrpop-docgen \
	  --lint \
	  -rp "./" \
	  -mp static/language/prolog \
          -me static/language/epilog \
          -gc "ModuleFile,Deploy,Query,Script" \
          --out-dir docs/language \
          tremor-script/src/grammar.lalrpop
	if test -f docs/language/grammar.md; then  mv docs/language/grammar.md docs/language/EBNF.md; fi
	if test -f docs/language/modulefile.md; then mv docs/language/modulefile.md docs/language/module_system.md; fi


pdf-doc: lalrpop-doc
	-mkdir docs/pdf
	cd docs && \
	echo pandoc --toc -f gfm  --verbose \
	  -F /Users/dennis/.nvm/versions/node/v16.13.0/bin/mermaid-filter \
	  --pdf-engine /usr/local/texlive/2021/bin/universal-darwin/xelatex \
	  --variable mainfont="Helvetica" \
	  --variable sansfont="Courier New" \
	  --syntax-definition=../tremor.xml \
	  --syntax-definition=../trickle.xml \
	  --syntax-definition=../troy.xml \
	  --syntax-definition=../ebnf.xml \ 
	  -V papersize=a4 -V geometry=margin=2cm \
	  --columns 80 --wrap auto \
	  --highlight-style=tango --include-in-header ../chapter.tex \
	  language.md language/full.md language/EBNF.md \
	  -o pdf/tremor-langauge-reference.pdf
	cd docs/library && \
	pandoc --toc -f gfm \
	  --pdf-engine xelatex  \
	  --variable mainfont="Helvetica" \
	  --variable sansfont="Courier New" \
	  --highlight-style=haddock  \
	  overview.md \
	  std.md std/*.md std/time/*.md std/integer/*.md \
	  tremor.md tremor/*.md \
          cncf.md cncf/otel.md \
	  cncf/otel/span_id.md cncf/otel/trace_id.md \
	  cncf/otel/logs.md cncf/otel/logs/*.md \
	  cncf/otel/metrics.md cncf/otel/metrics/*.md \
	  cncf/otel/trace.md cncf/otel/trace/status.md cncf/otel/trace/status/*.md cncf/otel/trace/spankind.md \
	  overview.md aggr.md aggr/stats.md aggr/win.md \
	  -o ../pdf/tremor-library-reference.pdf
	  

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
