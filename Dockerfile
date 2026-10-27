# Static, no-CGO binary on an empty base: ~10 MB, no shell, runs as uid 1001.
FROM golang:1.27.3-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /flag-file-relay .

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /flag-file-relay /usr/local/bin/flag-file-relay
USER 1001:1001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/flag-file-relay"]
CMD ["serve"]
