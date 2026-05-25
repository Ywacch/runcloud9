FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o loadgen .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /
COPY --from=builder /app/loadgen /loadgen
EXPOSE 9100
CMD ["/loadgen"]
