# Build from repo root: docker build -f images/fanout-worker/Dockerfile .
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg ./pkg
COPY images/fanout-worker/go.mod images/fanout-worker/go.sum ./images/fanout-worker/
WORKDIR /src/images/fanout-worker
RUN go mod download
COPY images/fanout-worker/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /fanout-worker .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /fanout-worker /fanout-worker
EXPOSE 8080
CMD ["/fanout-worker"]
