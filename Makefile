DOCKER_IMAGE := fgrehm/alpine-go-web:1.5.1
DOCKER_DEV_CONTAINER_NAME := simple-json-db-dev

all: build test

.PHONY: build
build: bin/sjdb

bin/sjdb: $(shell find -L src -type f -name '*.go')
	gb build cmd/sjdb-cli

.PHONY: test
test:
	@echo 'Running tests...'
	gb test ...

.PHONY: test.watch
test.watch:
	$(MAKE) test || true
	watchf -e "write,remove,create" -c "clear" -c "make test" -include ".go$$" -r

.PHONY: watch
watch:
	$(MAKE) build || true
	$(MAKE) test || true
	watchf -e "write,remove,create" -c "clear" -c "make build test" -include ".go$$" -r

.PHONY: fmt
fmt: src/**/*.go
	go fmt ./...

.PHONY: hack
hack:
	docker run \
					-ti \
					--rm \
					--name $(DOCKER_DEV_CONTAINER_NAME) \
					-v `pwd`:/code \
					-w /code \
					$(DOCKER_IMAGE) \
					bash
