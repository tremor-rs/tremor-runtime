FROM rust:1.46.0 as builder

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y libclang-dev cmake  \
    #
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Switch back to dialog for any ad-hoc use of apt-get
ENV DEBIAN_FRONTEND=dialog
ENV RUSTFLAGS="-C target-feature=+avx,+avx2,+sse4.2"

COPY Cargo.* ./

# Main library
COPY src ./src
# supporting libraries
COPY tremor-pipeline ./tremor-pipeline
COPY tremor-script ./tremor-script
COPY tremor-api ./tremor-api
COPY tremor-influx ./tremor-influx
# Binaries
COPY tremor-cli ./tremor-cli
COPY tremor-common ./tremor-common

RUN cat /proc/cpuinfo
RUN cargo build --release --all --verbose

FROM debian:buster-slim

RUN apt-get update \
    && apt-get install -y libssl1.1 libcurl4 libatomic1 \
    #
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder target/release/tremor /tremor

# stdlib
RUN mkdir -p /opt/local/tremor/lib
COPY tremor-script/lib /opt/local/tremor/lib

# Entrypoint
COPY docker/entrypoint.sh /entrypoint.sh
# configuration file
RUN mkdir /etc/tremor
COPY docker/config /etc/tremor/config
# logger configuration
COPY docker/logger.yaml /etc/tremor/logger.yaml

ENV TREMOR_PATH=/opt/local/tremor/lib

ENTRYPOINT ["/entrypoint.sh"]
