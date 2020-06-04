# Packaging

Collects all the resources necessary for successfully packaging our rust project for various [targets](https://doc.rust-lang.org/rustc/targets/built-in.html), across different formats.

## Usage

The main packaging functionality here is exposed via [run.sh](run.sh) -- please refer to the script header there for more details on its usage.

For CI or local use, packaging is best done via the [project Makefile](../Makefile), which has convenient (make) targets defined for doing it against all the supported rustc targets as well as package formats.

```bash
make builder-images && make packages # from project root
```

Or to package for a specific target only (across all release formats):

```bash
make builder-image-x86_64-unknown-linux-gnu && make package-x86_64-unknown-linux-gnu
```

Resulting artifacts will be available in the directory `packaging/out`, relative to project root. Enjoy!

Note: once we have the builder images successfuly pushed to [docker hub](https://hub.docker.com/r/tremorproject/tremor-builder), just `make packages` will suffice (the images will be pulled in automatically as part of project build).

Other examples:

```bash
# produce packages of all supported formats (using glibc based binaries) for x86_64 linux
./cross_build.sh x86_64-unknown-linux-gnu && ./run.sh x86_64-unknown-linux-gnu

# produce archive (using musl based binaries) for x86_64 linux
./cross_build.sh x86_64-alpine-linux-musl && ./run.sh -f archive x86_64-alpine-linux-musl
```

### Requirements

* bash
* git
* cargo
* docker
* dpkg, ldd (optionally, to auto-infer dynamic lib dependencies during debian packaging, via [cargo-deb](https://github.com/mmstick/cargo-deb#installation))

The setup here was tested successfully from linux (ubuntu) environments, but it should work well in other environments too, as long as the above requirements are met.

## Supported Targets

* x86_64-unknown-linux-gnu
* x86_64-alpine-linux-musl

For the list of targets used during project release, please refer to the [project Makefile](../Makefile).

## Supported Formats

* archive ([tar.gz](https://en.wikipedia.org/wiki/Tar_(computing)) for linux targets)
* deb ([Debian package](https://www.debian.org/doc/debian-policy/ch-binary.html))

For the list of formats used during project release (variable by target), please refer to the [project Makefile](../Makefile).
