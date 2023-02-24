.PHONY: default
default: all

.PHONE: all
all: build test

.PHONY: clean
clean:
	go clean ./...

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go test -count=1 ./...
	./fuzz.sh

.PHONY: test-v
test-v:
	GOLOG_LOG_LEVEL=debug go test -count=1 -v ./...
	GOLOG_LOG_LEVEL=debug ./fuzz.sh

.PHONY: watch
watch:
	watchexec -c "make test"

.PHONY: lint
lint: ## Run style checks and verify syntax
	go vet -asmdecl -assign -atomic -bools -buildtag -cgocall -copylocks -httpresponse -loopclosure -lostcancel -nilfunc -printf -shift -stdmethods -structtag -tests -unmarshal -unreachable -unsafeptr -unusedresult ./...
	test -z $(gofmt -l ./...)