# Packaging

Collects all the resources necessary for successfully packaging our rust project for various [targets](https://doc.rust-lang.org/rustc/targets/built-in.html), across different formats.

## Usage

The main packaging functionality here is exposed via [run.sh](run.sh) -- please refer to the script header there for more details on its usage.

For CI or local use, packaging is best done via the [project Makefile](../Makefile), which has convenient (make) targets defined for doing it against all the supported rustc targets as well as package formats.

```bash
make packages # from project root
```

Or to package for a specific target only (across all release formats):

```bash
make package-x86_64-unknown-linux-gnu # from project root
```

Resulting artifacts will be available in the directory `packaging/out`, relative to project root. Enjoy!

Other examples, using the scripts here directly:

```bash
# produce packages of all supported formats (using glibc based binaries) for x86_64 linux
./cross_build.sh x86_64-unknown-linux-gnu && ./run.sh x86_64-unknown-linux-gnu

# produce archive (using musl based binaries) for x86_64 linux
./cross_build.sh x86_64-alpine-linux-musl && ./run.sh -f archive x86_64-alpine-linux-musl
```

If you need to build the docker images used during the cross build process, please refer to the documentation on [builder-images](./builder-images).

### Requirements

* bash
* git
* cargo
* docker
* dpkg, ldd (optionally, to auto-infer dynamic lib dependencies during debian packaging, via [cargo-deb](https://github.com/mmstick/cargo-deb#installation))
* rpmbuild (for building rpms, via [cargo-rpm](https://github.com/iqlusioninc/cargo-rpm.git))

The setup here was tested successfully from linux (ubuntu) environments, but it should work well in other environments too, as long as the above requirements are met.

## Supported Targets

* x86_64-unknown-linux-gnu
* x86_64-alpine-linux-musl

For the list of targets used during project release, please refer to the [project Makefile](../Makefile).

## Supported Formats

* archive ([tar.gz](https://en.wikipedia.org/wiki/Tar_(computing)) for linux targets)
* deb ([Debian package](https://www.debian.org/doc/debian-policy/ch-binary.html))
* rpm ([RPM package](https://rpm.org/))

For the list of formats used during project release (variable by target), please refer to the [project Makefile](../Makefile). A summary is given down below too.

## Release Compatibility

| Target | Format | Supported Platforms |
| -------| -------| --------------------|
| x86_64-unknown-linux-gnu | archive | Should work on linux systems with glibc (>=2.17), libstdc++, libatomic, libssl (==1.0.2) |
| x86_64-unknown-linux-gnu | deb | ~Debian jessie/stretch/buster as well as its derivatives like Ubuntu~ TODO |
| x86_64-unknown-linux-gnu | rpm | Centos7/8 |
| x86_64-alpine-linux-musl | archive | Should work on all linux systems but not as [performant](https://github.com/tremor-rs/tremor-runtime/issues/377) as the gnu releases |
