FROM golang:1.24 AS builder
WORKDIR /src
COPY go.mod dispenser.go runtime.go hostd.go controld.go cli.go ui.go main.go Dockerfile deploy.sh service-control-plane.yaml service-pool.yaml service-ui.yaml ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/dispenser .

FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl python3 tar procps git nodejs npm postgresql postgresql-client sudo && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /workspace /persistent /pgdata/data /var/run/postgresql /app /mnt/shared /mnt/gcs && \
    chmod 777 /var/run/postgresql
COPY --from=builder /out/dispenser /app/dispenser
RUN ln -sf /app/dispenser /usr/local/bin/dispenser && \
    ln -sf /app/dispenser /usr/local/bin/sbx && \
    ln -sf /app/dispenser /usr/local/bin/gcloud-run-sandbox
WORKDIR /workspace
EXPOSE 8080
ENTRYPOINT ["/app/dispenser"]
