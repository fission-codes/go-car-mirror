.PHONY: test

default: all

all: build test

clean:
	go clean ./...
	rm -rf testdata

generate:
	go generate ./...

build: generate
	go build ./...

test: generate
	go test -count=1 ./...
	./fuzz.sh

test-v:
	GOLOG_LOG_LEVEL=debug go test -count=1 -v ./...
	GOLOG_LOG_LEVEL=debug ./fuzz.sh

watch:
	watchexec -c "make test"