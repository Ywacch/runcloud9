# Build from repo root: docker build -f images/loadgen/Dockerfile .
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg ./pkg
COPY images/loadgen/go.mod images/loadgen/go.sum ./images/loadgen/
WORKDIR /src/images/loadgen
RUN go mod download
COPY images/loadgen/main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /loadgen .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /loadgen /loadgen
EXPOSE 9100
CMD ["/loadgen"]
