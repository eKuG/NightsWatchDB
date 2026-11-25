FROM golang:1.27.4-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /bake .

FROM debian:bookworm-slim
COPY --from=build /bake /usr/local/bin/bake
ENTRYPOINT ["bake"]
