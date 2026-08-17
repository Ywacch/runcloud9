# Build from this directory (not repo root):
#   docker build -t us-central1-docker.pkg.dev/ganymede-cloud/cloud9-images/social-feed:${TAG} .
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o social-feed .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /app/social-feed .
EXPOSE 8080
CMD ["./social-feed"]
