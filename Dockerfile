FROM rust:1.100.0-bookworm@sha256:75d26c23c24cc6ea1cdabf0714a6bff8fd7bf3248716043fd7ae86dac7f41b94 AS build
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

FROM debian:bookworm-slim@sha256:e67eeead736ff7bbe258ef088bff683635c561b1fcfefad0806c1df4ab49ce0e
RUN useradd --system --uid 10001 --home-dir /var/lib/nightswatchdb --shell /usr/sbin/nologin nwdb \
    && mkdir -p /var/lib/nightswatchdb \
    && chown nwdb /var/lib/nightswatchdb
COPY --from=build /src/target/release/nightswatchdb /usr/local/bin/nightswatchdb
USER nwdb
VOLUME /var/lib/nightswatchdb
EXPOSE 4317 4320
# The binary listens on loopback by default because it has no authentication
# or TLS. In a container the published ports are the access control, so it
# listens on every interface here. The API's Host check is on all the same: a
# client that reaches it by a name (a container name on a Docker network) needs
# NWDB_API_ALLOWED_HOSTS; an IP address or localhost always works.
ENTRYPOINT ["nightswatchdb", \
    "--data-dir", "/var/lib/nightswatchdb", \
    "--otlp-grpc-addr", "0.0.0.0:4317", \
    "--api-addr", "0.0.0.0:4320"]
