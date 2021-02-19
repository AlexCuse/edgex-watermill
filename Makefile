.PHONY: test

GO=CGO_ENABLED=1 GO111MODULE=on go

test-core:
	cd core && $(GO) test ./...
	cd core && $(GO) vet ./...
	gofmt -l core/
	[ "`gofmt -l core/`" = "" ]

test-kafka:
	cd kafka $(GO) test ./...
	cd kafka && $(GO) vet ./...
	gofmt -l kafka/
	[ "`gofmt -l kafka/`" = "" ]

test-amqp:
	cd amqp $(GO) test ./...
	cd amqp && $(GO) vet ./...
	gofmt -l amqp/
	[ "`gofmt -l amqp/`" = "" ]

test-nats:
	cd nats $(GO) test ./...
	cd nats && $(GO) vet ./...
	gofmt -l nats/
	[ "`gofmt -l nats/`" = "" ]

test: test-core test-kafka test-amqp test-nats