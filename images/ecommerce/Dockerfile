# Build from repo root: docker build -f images/ecommerce/Dockerfile .
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg ./pkg
COPY images/ecommerce/go.mod images/ecommerce/go.sum ./images/ecommerce/
WORKDIR /src/images/ecommerce
RUN go mod download
COPY images/ecommerce/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /ecommerce .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /ecommerce /ecommerce
EXPOSE 8080
CMD ["/ecommerce"]
