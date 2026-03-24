FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/korpus .

FROM alpine:3.21
RUN adduser -D -u 10001 korpus
WORKDIR /app

COPY --from=builder /bin/korpus /usr/local/bin/korpus

USER korpus
EXPOSE 4222

ENTRYPOINT ["/usr/local/bin/korpus"]
