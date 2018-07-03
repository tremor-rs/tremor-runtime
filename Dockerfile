FROM rust:1.30.1
RUN apt update && apt install -y bison flex vim automake cmake
WORKDIR /home/rust/src
COPY Cargo.* /home/rust/src/
COPY src /home/rust/src/src
COPY window /home/rust/src/window
RUN cargo build --release
RUN cp target/release/tremor-runtime /home/rust
WORKDIR /home/rust
RUN rm -rf src
COPY tremor-runtime.sh /home/rust
ENTRYPOINT ["/home/rust/tremor-runtime.sh"]
