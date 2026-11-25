FROM rust:1.100.0-bookworm AS build
WORKDIR /src
# dependency layer: rebuilt only when Cargo.toml or Cargo.lock change
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src target/release/textindex* target/release/deps/textindex*
COPY src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/textindex /usr/local/bin/textindex
ENTRYPOINT ["textindex"]
