FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o cloud9-api ./cmd/api

FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/cloud9-api /cloud9-api
ENTRYPOINT ["/cloud9-api"]
