# Default build (macOS)
go build -o ./vgnms-macos v-gnms

# Build for Linux
env GOOS=linux GOARCH=amd64 GOARM=7 go build -o ./vgnms-linux v-gnms

# Compress necessary files for deployment
zip -r ./VGNMS-BUILD.zip ./docker-compose.yml ./vgnms-linux ./vgnms-macos ./dist ./rabbitmq ./rbac_model.conf