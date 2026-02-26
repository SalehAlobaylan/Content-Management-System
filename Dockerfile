# Build stage
FROM golang:1.24-alpine AS builder

# Install git for fetching dependencies
RUN apk add --no-cache git

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary with optimizations
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/server ./src/main.go

# Production stage
FROM alpine:3.19

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/server .

# Create non-root user for security
RUN adduser -D -g '' appuser
USER appuser

# Environment variables
ENV PORT=8080
ENV GIN_MODE=release

EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD sh -c 'wget --no-verbose --tries=1 --spider "http://localhost:${PORT:-8080}/health" || exit 1'

CMD ["./server"]
