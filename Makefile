.PHONY: test

default: all

all: build test

clean:
	go clean ./...
	rm -rf testdata

build:
	go build ./...

test:
	go test -v -count=1 ./...
	./fuzz.sh

watch:
	watchexec -c "make test"