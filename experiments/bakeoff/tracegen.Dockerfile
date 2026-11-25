FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY . .
RUN go build -o /tracegen .

FROM debian:bookworm-slim
COPY --from=build /tracegen /usr/local/bin/tracegen
ENTRYPOINT ["tracegen"]
