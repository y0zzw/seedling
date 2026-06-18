# Use the official Go image as the base image
FROM golang:1.23-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the source code into the container
COPY . .

# Build the application
RUN GOOS=linux GOARCH=amd64 GOARM=7 go build -o ./vgnms .

# Use a minimal alpine image for the final stage
FROM alpine:latest

# Set the working directory inside the container
WORKDIR /root/

# Add timezone support
RUN apk update && apk add --no-cache tzdata

# Copy the binary from the builder stage
COPY --from=builder /app/vgnms .

# Copy the dist folder
# COPY --from=builder /app/dist ./dist

# Copy the .env file
# COPY .env .

# Copy the rbac_model.conf file
COPY rbac_model.conf .

# Expose the port the app runs on
EXPOSE 8080

# Command to run the executable
CMD ["./vgnms"]
