FROM ubuntu
RUN apt-get update && apt-get install -y pkg-config cmake zlib1g-dev curl libssl1.0-dev libssl1.0 libclang-dev clang
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
RUN . ~/.cargo/env && cargo install cargo-tarpaulin -f
COPY docker/tarpaulin.sh /
ENTRYPOINT ["/tarpaulin.sh"]
