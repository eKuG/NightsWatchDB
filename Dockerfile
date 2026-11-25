FROM rust:1.100.0-bookworm AS build
# The repository's toolchain file asks for rustfmt and clippy, which a release
# build does not need and would download.
ENV RUSTUP_TOOLCHAIN=1.100.0
WORKDIR /src
COPY . .
# Only downloads are cached between builds. Compiled output is not: cargo
# decides what is up to date by file times, and a cache shared by builds of
# different checkouts would hand back objects compiled from other source.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --locked -p nightswatchdb

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --home-dir /var/lib/nightswatchdb --shell /usr/sbin/nologin nwdb \
    && mkdir -p /var/lib/nightswatchdb \
    && chown nwdb /var/lib/nightswatchdb
COPY --from=build /src/target/release/nightswatchdb /usr/local/bin/nightswatchdb
USER nwdb
VOLUME /var/lib/nightswatchdb
EXPOSE 4317 4320
# The binary listens on loopback by default because it has no authentication
# or TLS. In a container the published ports are the access control, so it
# listens on every interface here.
ENTRYPOINT ["nightswatchdb", \
    "--data-dir", "/var/lib/nightswatchdb", \
    "--otlp-grpc-addr", "0.0.0.0:4317", \
    "--api-addr", "0.0.0.0:4320"]
