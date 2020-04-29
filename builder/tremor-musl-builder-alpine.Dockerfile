# build via:
#   docker build --network host -t tremorproject/tremor-musl-builder-alpine:latest -f ./tremor-musl-builder-alpine.Dockerfile .

# edge enables us to use latest rustc
# (latest image has older rustc which does not work for some of our dependencies
# installing rust from edge also does not work in the latest image, without upgrading
# the whole system).
FROM alpine:edge
# or use a date tag, via https://hub.docker.com/_/alpine
#FROM alpine:20200319

RUN apk update && apk add \
    # basics
    rust cargo \
    # dependencies for C builds
    # TODO maybe just install build-base here
    cmake make g++ \
    # for grok (via onig-sys)
    clang \
    # for openssl (via surf). static package needed for the final linking
    openssl-dev openssl-libs-static \
    # for mimalloc
    linux-headers

# only alpine's patched rustc has worked for succesful tremor static builds
# so installing latest rust this way does not quite work
#
#RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup-init.sh \
#    #&& sh rustup-init.sh -y --no-modify-path \
#    && sh rustup-init.sh -y \
#    && rm rustup-init.sh
#
#ENV PATH="/root/.cargo/bin:${PATH}"
#
#RUN rustup target add x86_64-unknown-linux-musl
