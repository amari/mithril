# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM golang:1.25 AS builder

# Enable Go modules and set target architecture
ARG TARGETOS
ARG TARGETARCH

# Set working directory
WORKDIR /app

# Copy Go module files
COPY go.mod go.sum ./

# Cache dependencies
RUN go mod download

# Copy the rest of the source code
COPY mithril-node-go/ mithril-node-go/
COPY gen/go/ gen/go/

# Build the Go binary for the target platform
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /bin/mithril-node-go ./mithril-node-go

# ---- Runtime stage ----
FROM debian:bookworm-slim AS runner

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /bin/mithril-node-go /usr/local/bin/mithril-node-go

# Create a non-root user to run the application
RUN useradd -r -s /bin/false mithril

# Create runtime directory
RUN mkdir -p /run/mithril && \
    chown mithril:mithril /run/mithril && \
    chmod 0755 /run/mithril

# Create data directory
RUN mkdir -p /var/lib/mithril && \
    chown mithril:mithril /var/lib/mithril && \
    chmod 0755 /var/lib/mithril

# Create etc directory
RUN mkdir -p /etc/mithril && \
    chown mithril:mithril /etc/mithril && \
    chmod 0755 /etc/mithril

# Switch to the non-root user
#USER mithril

# Set environment variables
ENV MITHRIL_ADMIN_SOCKET=/run/mithril/admin.sock
ENV MITHRIL_DATA_DIR=/var/lib/mithril
ENV MITHRIL_LOG_LEVEL=info

# Run the command by default
CMD ["/usr/local/bin/mithril-node-go"]
