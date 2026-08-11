# Lightweight, static, no-CGO — final image is ~15MB.
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /flag-file-relay .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates && adduser -u 1001 -D relay
COPY --from=build /flag-file-relay /usr/local/bin/flag-file-relay
USER relay
EXPOSE 8080
ENTRYPOINT ["flag-file-relay"]
CMD ["serve"]
