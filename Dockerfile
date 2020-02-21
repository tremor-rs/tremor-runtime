FROM rust:latest as builder

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

COPY Cargo.* ./
COPY .cargo ./.cargo
# Main library
COPY src ./src
# supporting libraries
COPY tremor-pipeline ./tremor-pipeline
COPY tremor-script ./tremor-script
COPY tremor-api ./tremor-api
# Binaries
COPY tremor-query ./tremor-query
COPY tremor-server ./tremor-server
COPY tremor-tool ./tremor-tool

RUN cargo build --release --all

FROM debian:buster-slim

RUN apt-get update \
    && apt-get install -y libssl1.1 \
    #
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*


COPY --from=builder target/release/tremor-server /tremor-server
COPY --from=builder target/release/tremor-tool /tremor-tool

# Entrypoint
COPY docker/entrypoint.sh /entrypoint.sh
# configuration file
RUN mkdir /etc/tremor
COPY docker/config /etc/tremor/config
# logger configuration
COPY docker/logger.yaml /etc/tremor/logger.yaml

ENTRYPOINT ["/entrypoint.sh"]
