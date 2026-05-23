FROM rust:latest AS builder
RUN apt-get update && apt-get install -y protobuf-compiler

WORKDIR /app
COPY . .


RUN #cargo install sccache --locked

RUN cargo build --release

RUN mkdir /config

ENTRYPOINT ["/app/target/release/vlinderd"]