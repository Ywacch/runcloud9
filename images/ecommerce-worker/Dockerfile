# Build from repo root: docker build -f images/ecommerce-worker/Dockerfile .
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg ./pkg
COPY images/ecommerce-worker/go.mod images/ecommerce-worker/go.sum ./images/ecommerce-worker/
WORKDIR /src/images/ecommerce-worker
RUN go mod download
COPY images/ecommerce-worker/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /ecommerce-worker .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /ecommerce-worker /ecommerce-worker
EXPOSE 8080
CMD ["/ecommerce-worker"]
