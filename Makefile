export SHELL := /bin/bash
export LOCAL_MODULE := github.com/WingT/go-grpc-cache
.PHONY: lint
lint: deps
	@go fmt ./...
	@goimports -local $(LOCAL_MODULE) -w .
	@go mod tidy
.PHONY: vet
vet:
	@go vet ./...
.PHONY: deps
deps:
	@hash goimports > /dev/null 2>&1; if [ $$? -ne 0 ]; then \
		go get golang.org/x/tools/cmd/goimports; \
	fi
.PHONY: test
test: style_test function_test
.PHONY: style_test
style_test: deps
	test -z "$$(gofmt -l .)"
	test -z "$$(goimports -local ${LOCAL_MODULE} -d .)"
function_test:
	go test ./...
.PHONY: gen
gen:
	@mkdir -p test_pb
	@protoc --go_out=paths=source_relative,plugins=grpc:./test_pb *.proto
	@make
