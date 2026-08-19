# Build from repo root: docker build -f images/caching-proxy/Dockerfile .
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg ./pkg
COPY images/caching-proxy/go.mod images/caching-proxy/go.sum ./images/caching-proxy/
WORKDIR /src/images/caching-proxy
RUN go mod download
COPY images/caching-proxy/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /caching-proxy .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /caching-proxy /caching-proxy
EXPOSE 8081
CMD ["/caching-proxy"]
