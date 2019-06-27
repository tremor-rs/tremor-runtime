FROM centos:7 as builder

ARG rust_version=1.35.0
RUN yum install git make gcc clang openssl-static libstdc++-static bison  autoconf -y
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain $rust_version -y

COPY Cargo.* ./
COPY .cargo ./.cargo
# Main library
COPY src ./src
# supporting libraries
COPY property_testing ./property_testing
COPY window ./window
COPY tremor-pipeline ./tremor-pipeline
COPY tremor-script ./tremor-script
COPY tremor-api ./tremor-api
COPY dissect ./dissect
COPY kv ./kv
# Binaries
COPY tremor-server ./tremor-server
COPY tremor-tool ./tremor-tool

RUN source $HOME/.cargo/env &&\
  cargo build --release --all

FROM centos:7
ARG rust_version=stable

# Debug / perf tooling
RUN yum install lldb git make gcc clang openssl-static libstdc++-static bison autoconf perf -y && yum clean all
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain $rust_version -y

COPY --from=builder target/release/tremor-server /tremor-server
COPY --from=builder target/release/tremor-tool /tremor-tall
# COPY --from=builder target/release/native/php-src/libs/libphp7.la /lib64
# COPY --from=builder target/release/native/php-src/libs/libphp7.so /lib64
# Entrypoint
COPY docker/entrypoint.sh /entrypoint.sh
# configuration file
RUN mkdir /etc/tremor
COPY docker/config /etc/tremor/config
# logger configuration
COPY docker/logger.yaml /etc/tremor/logger.yaml
# static files
COPY static /static

ENTRYPOINT ["/entrypoint.sh"]
