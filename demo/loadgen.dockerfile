
FROM artifactory.service.bo1.csnzoo.com/external-staging/ekidd/rust-musl-builder:1.27.0 as builder
RUN sudo apt update && sudo apt install -y bison flex automake
WORKDIR /home/rust/src
COPY Cargo.* /home/rust/src/
COPY src /home/rust/src/src
RUN find
RUN LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu CFLAGS=-I/usr/local/musl/include CC=musl-gcc PREFIX=/usr/local/musl cargo build --release

FROM artifactory.service.bo1.csnzoo.com/external/alpine:3.6


WORKDIR /root/
RUN apk --no-cache add ca-certificates
COPY demo/loadgen.sh .
COPY demo/data.json.xz .
COPY --from=builder /home/rust/src/target/x86_64-unknown-linux-musl/release/tremor-runtime tremor-runtime

# This image runs SimpleHTTPServer when the container starts.
#
# 9.  Change this to a command which starts your application.
#
CMD ["./loadgen.sh"]
