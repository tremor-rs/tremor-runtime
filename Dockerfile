FROM centos:7 as builder

ARG rust_version=stable
RUN yum install git make gcc clang openssl-static libstdc++-static bison  autoconf -y
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain $rust_version -y

COPY Cargo.* ./
COPY src ./src
COPY window ./window
COPY mimir ./mimir

RUN source $HOME/.cargo/env &&\
  cargo build --release

FROM centos:7

COPY --from=builder target/release/tremor-runtime /tremor-runtime
COPY --from=builder target/release/native/php-src/libs/libphp7.la /lib64
COPY --from=builder target/release/native/php-src/libs/libphp7.so /lib64
COPY tremor-runtime.sh /tremor-runtime.sh
COPY tremor.yaml /tremor.yaml

ENTRYPOINT ["/tremor-runtime.sh"]
