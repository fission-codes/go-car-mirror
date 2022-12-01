.PHONY: test

default: all

all: build test

clean:
	go clean ./...
	rm -rf testdata

build:
	go build ./...

test:
	go test -v ./...
	./fuzz.sh

watch:
	watchexec -c "make test"