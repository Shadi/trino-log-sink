# syntax=docker/dockerfile:1

FROM golang:1.24 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/trino-query-log-sink ./cmd/trino-query-log-sink

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/trino-query-log-sink /usr/local/bin/trino-query-log-sink
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/trino-query-log-sink"]
CMD ["serve"]
