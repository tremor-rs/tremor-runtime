FROM centos:7 as builder

ARG rust_version=1.40.0
RUN yum install centos-release-scl -y && \
    yum install devtoolset-8-gcc devtoolset-8-gcc-c++ jq git2u make gcc clang openssl-static libstdc++-static bison autoconf -y && \
    yum clean all
RUN curl -OL https://github.com/Kitware/CMake/releases/download/v3.15.0/cmake-3.15.0.tar.gz && \
    tar -xzf cmake-3.15.0.tar.gz && \
    cd cmake-3.15.0 && \
    ./bootstrap && \
    make && \
    make install && \
    cd .. && rm -rf cmake-3.15.0 cmake-3.15.0.tar.gz
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain $rust_version -y

COPY Cargo.* ./
COPY .cargo ./.cargo
# Main library
COPY src ./src
# supporting libraries
COPY window ./window
COPY tremor-pipeline ./tremor-pipeline
COPY tremor-script ./tremor-script
COPY tremor-api ./tremor-api
COPY dissect ./dissect
COPY kv ./kv
# Binaries
COPY tremor-query ./tremor-query
COPY http-bench-server ./http-bench-server
COPY tremor-server ./tremor-server
COPY tremor-tool ./tremor-tool

RUN source $HOME/.cargo/env &&\
    source /opt/rh/devtoolset-8/enable &&\
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
