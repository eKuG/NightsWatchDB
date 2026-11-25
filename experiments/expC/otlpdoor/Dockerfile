FROM rust:1.100.0-bookworm AS build
WORKDIR /src
# dependency layer: only rebuilt when Cargo.toml or Cargo.lock change
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src target/release/otlpdoor* target/release/deps/otlpdoor*
COPY src ./src
# source files carry old mtimes; make cargo see them as newer than the stub build
RUN touch src/*.rs
RUN cargo build --release --locked

# docker build --target test .   runs the unit tests on linux/arm64
FROM build AS test
RUN cargo test --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/otlpdoor /usr/local/bin/otlpdoor
ENTRYPOINT ["otlpdoor"]
