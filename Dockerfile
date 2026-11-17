FROM golang:1.25-alpine AS builder
WORKDIR /app

ARG VERSION=dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X main.version=${VERSION}" -o cloud9-api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o cloud9-reaper ./cmd/reaper

FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/cloud9-api /cloud9-api
COPY --from=builder /app/cloud9-reaper /cloud9-reaper
COPY --from=builder /app/config/image-versions.json /config/image-versions.json
ENTRYPOINT ["/cloud9-api"]
