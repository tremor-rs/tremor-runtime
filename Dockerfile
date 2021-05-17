FROM rust:1.51.0 as builder

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
ENV RUSTFLAGS="-C target-feature=+avx,+avx2,+sse4.2"

WORKDIR /app

COPY Cargo.* /app/

# We change lto to 'thin' for docker builds so it
# can be build on more moderate system
RUN mv Cargo.toml Cargo.toml.orig && sed 's/lto = true/lto = "thin"/' Cargo.toml.orig > Cargo.toml

# Main library
COPY src /app/src
COPY build.rs /app/build.rs
COPY .cargo /app/.cargo
# supporting libraries
COPY tremor-pipeline /app/tremor-pipeline
COPY tremor-script /app/tremor-script
COPY tremor-api /app/tremor-api
COPY tremor-influx /app/tremor-influx
COPY tremor-value /app/tremor-value
# Binaries
COPY tremor-cli /app/tremor-cli
COPY tremor-common /app/tremor-common
# Git info to track version
COPY .git /app/.git

RUN cat /proc/cpuinfo
RUN cargo build --release --all --verbose
RUN strip target/release/tremor

FROM debian:buster-slim

RUN useradd -ms /bin/bash tremor

RUN apt-get update \
    && apt-get install -y libssl1.1 libcurl4 libatomic1 tini curl \
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

HEALTHCHECK --interval=30s --timeout=1s --start-period=5s --retries=3 CMD curl -f http://localhost:9898/version || exit 1