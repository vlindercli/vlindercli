FROM rust:latest

WORKDIR /app
COPY . .

RUN apt-get update && apt-get install -y protobuf-compiler
RUN cargo install sccache --locked

RUN cargo build --release

RUN mkdir /config

ENTRYPOINT ["/app/target/release/vlinderd"]