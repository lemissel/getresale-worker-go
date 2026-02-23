# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install git for dependencies if needed
RUN apk add --no-cache git

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o worker .

# Final stage
FROM alpine:3.19

WORKDIR /app

# Install ca-certificates for HTTPS calls to Gemini
RUN apk add --no-cache ca-certificates tzdata

# Copy binary from builder
COPY --from=builder /app/worker .
COPY --from=builder /app/.env.example .env

# Create a non-root user
RUN adduser -D appuser
USER appuser

CMD ["./worker"]
