# Build from repo root: docker build -f images/social-feed/Dockerfile .
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg ./pkg
COPY images/social-feed/go.mod images/social-feed/go.sum ./images/social-feed/
WORKDIR /src/images/social-feed
RUN go mod download
COPY images/social-feed/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /social-feed .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /social-feed /social-feed
EXPOSE 8080
CMD ["/social-feed"]
