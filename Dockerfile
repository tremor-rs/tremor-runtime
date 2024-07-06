FROM rust:1.79-bookworm as builder

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y libclang-dev cmake git \
    #
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Switch back to dialog for any ad-hoc use of apt-get
ENV DEBIAN_FRONTEND=dialog
ENV RUSTFLAGS="-C target-feature=+avx,+avx2,+sse4.2 --cfg tokio_unstable"

WORKDIR /app

COPY Cargo.* /app/

# Main library
COPY src /app/src
COPY build.rs /app/build.rs
COPY .cargo /app/.cargo
# supporting libraries
COPY tremor-pipeline /app/tremor-pipeline
COPY tremor-script /app/tremor-script
COPY tremor-script-nif /app/tremor-script-nif
COPY tremor-api /app/tremor-api
COPY tremor-influx /app/tremor-influx
COPY tremor-value /app/tremor-value
COPY tremor-config /app/tremor-config
COPY tremor-archive /app/tremor-archive
COPY tremor-connectors /app/tremor-connectors
COPY tremor-connectors-test-helpers /app/tremor-connectors-test-helpers
COPY tremor-connectors-object-storage /app/tremor-connectors-object-storage
COPY tremor-connectors-aws /app/tremor-connectors-aws
COPY tremor-connectors-azure /app/tremor-connectors-azure
COPY tremor-connectors-otel /app/tremor-connectors-otel
COPY tremor-connectors-gcp /app/tremor-connectors-gcp
COPY tremor-system /app/tremor-system
COPY tremor-codec /app/tremor-codec
COPY tremor-interceptor /app/tremor-interceptor
# Binaries
COPY tremor-cli /app/tremor-cli
COPY tremor-common /app/tremor-common
# Git info to track version
COPY .git /app/.git

RUN cat /proc/cpuinfo
RUN cargo build --release --all --verbose
RUN strip target/release/tremor

FROM debian:bookworm-slim

RUN useradd -ms /bin/bash tremor

RUN apt-get update \
    && apt-get install -y libatomic1 tini curl \
    #
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/tremor /tremor

# stdlib
RUN mkdir -p /usr/share/tremor/lib
COPY tremor-script/lib /usr/share/tremor/lib

# Entrypoint
COPY docker/entrypoint.sh /entrypoint.sh
# configuration file
RUN mkdir /etc/tremor
COPY docker/config /etc/tremor/config
# logger configuration
COPY docker/logger.yaml /etc/tremor/logger.yaml

# setting TREMOR_PATH
# /usr/local/share/tremor - for host-specific local tremor-script modules and libraries, takes precedence
# /usr/share/tremor/lib - place for the tremor-script stdlib
ENV TREMOR_PATH="/usr/local/share/tremor:/usr/share/tremor/lib"

ENTRYPOINT ["/entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=1s --start-period=5s --retries=3 CMD curl -f http://localhost:9898/v1/status || exit 1
