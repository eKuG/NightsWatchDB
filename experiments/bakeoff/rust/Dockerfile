FROM rust:1.100.0-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/bake /usr/local/bin/bake
ENTRYPOINT ["bake"]
