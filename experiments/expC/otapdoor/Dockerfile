FROM rust:1.100.0-bookworm AS build
WORKDIR /src
# dependency layer: build with a stub main so that only source changes rebuild the crate itself
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src target/release/otapdoor* target/release/deps/otapdoor*
COPY src ./src
# COPY keeps the sources' old mtimes, which would make cargo reuse the stub build
RUN touch src/*.rs && cargo build --release --locked

# `docker build --target test .` runs the unit tests inside the toolchain image
FROM build AS test
RUN cargo test --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/otapdoor /usr/local/bin/otapdoor
ENTRYPOINT ["otapdoor"]
