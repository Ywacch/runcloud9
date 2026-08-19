# Build from repo root: docker build -f images/url-shortener/Dockerfile .
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg ./pkg
COPY images/url-shortener/go.mod images/url-shortener/go.sum ./images/url-shortener/
WORKDIR /src/images/url-shortener
RUN go mod download
COPY images/url-shortener/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /url-shortener .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /url-shortener /url-shortener
EXPOSE 8080
CMD ["/url-shortener"]
