.PHONY: build frontend run test

frontend:
	cd ../client && npm ci && npm run build

build: frontend
	go build -o bin/limulus .

run: frontend
	go run .

test:
	go test ./...
