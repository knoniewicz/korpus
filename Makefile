.PHONY: run test fmt tidy docker-up docker-down example-up example-down

run:
	go run .

test:
	go test ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

tidy:
	go mod tidy

docker-up:
	docker compose up --build

docker-down:
	docker compose down -v

example-up:
	docker compose -f examples/docker-compose.yml up --build

example-down:
	docker compose -f examples/docker-compose.yml down -v
