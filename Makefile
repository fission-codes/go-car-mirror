.PHONY: test

default: all

all: build test

clean:
	go clean ./...
	rm -rf testdata

build:
	go build ./...

test:
	go test -count=1 ./...
	./fuzz.sh

test-v:
	GOLOG_LOG_LEVEL=debug go test -count=1 -v ./...
	GOLOG_LOG_LEVEL=debug ./fuzz.sh

test-diagram:
	go test -count=1 -v ./diagram/...

watch:
	watchexec -c "make test"