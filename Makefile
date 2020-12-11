.PHONY: test

GO=CGO_ENABLED=1 GO111MODULE=on go

test-core:
	$(GO) test ./...
	$(GO) vet ./...
	gofmt -l .
	[ "`gofmt -l .`" = "" ]

test-kafka:
	cd kafka $(GO) test ./...
	cd kafka && $(GO) vet ./...
	gofmt -l kafka/
	[ "`gofmt -l kafka/`" = "" ]

test: test-core test-kafka