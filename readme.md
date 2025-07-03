go build -o . ./cmd/server/
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o . ./cmd/agent/.