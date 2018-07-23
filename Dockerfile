FROM artifactory.service.bo1.csnzoo.com/external-staging/ekidd/rust-musl-builder:1.27.0 as builder
RUN sudo apt update && sudo apt install -y bison flex vim automake
WORKDIR /home/rust/src
COPY Cargo.* /home/rust/src/
COPY src /home/rust/src/src
RUN echo LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu CFLAGS=-I/usr/local/musl/include CC=musl-gcc PREFIX=/usr/local/musl cargo build --release
