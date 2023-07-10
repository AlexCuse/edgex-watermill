.PHONY: test

GO=CGO_ENABLED=1 GO111MODULE=on go

run-example:
	cd _example && docker-compose up

lint:
	golangci-lint run

test:
	go test ./...