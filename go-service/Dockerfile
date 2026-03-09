FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install build dependencies for SQLite (CGO)
RUN apk add --no-cache gcc musl-dev sqlite-dev

# Copy go module files
COPY go.mod ./

# Copy all source code
COPY . .

# Download dependencies and generate go.sum
RUN go mod tidy

# Build the application with CGO enabled for SQLite
RUN CGO_ENABLED=1 GOOS=linux go build -o sensor-service .

# Use alpine for the final stage (needs SQLite libraries)
FROM alpine:latest

WORKDIR /app

# Install SQLite runtime libraries
RUN apk add --no-cache sqlite-libs

# Copy the binary from the builder stage
COPY --from=builder /app/sensor-service .

# Expose the port the service will run on
EXPOSE 8080

# Run the service
CMD ["./sensor-service"]
