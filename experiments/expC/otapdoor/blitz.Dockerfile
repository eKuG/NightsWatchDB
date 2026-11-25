# Blitz v0.22.0 built from its Go module (git tag), for the logs and metrics runs. Context is not used.
FROM golang:1.26-bookworm AS build
RUN git clone --depth 1 --branch v0.22.0 https://github.com/observIQ/blitz.git /src
WORKDIR /src
RUN CGO_ENABLED=0 go build -o /blitz ./cmd/blitz

FROM debian:bookworm-slim
COPY --from=build /blitz /usr/local/bin/blitz
ENTRYPOINT ["blitz"]
