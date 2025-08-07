FROM golang:1.23-alpine AS builder

# Install dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy go mod files and source code
COPY . .

# Download dependencies and build
RUN go mod tidy && go mod download && CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o auditor .

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/auditor .

# Create non-root user
RUN addgroup -g 1001 -S auditor && \
    adduser -S -D -H -u 1001 -s /sbin/nologin -G auditor auditor

USER auditor

# Expose metrics port
EXPOSE 9091

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:9091/health || exit 1

CMD ["./auditor"]
