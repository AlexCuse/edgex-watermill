include golang.mk

.PHONY: test

GO=CGO_ENABLED=1 GO111MODULE=on go