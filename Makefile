DOCKER_IMAGE := fgrehm/simple-json-db
DOCKER_DEV_CONTAINER_NAME := simple-json-db-dev

all: build test

.PHONY: build
build: bin/simple-json-db

bin/simple-json-db: $(shell find -L src -type f -name '*.go')
	gb build cmd/simple-json-db

.PHONY: test
test:
	@echo 'Running tests...'
	gb test simplejsondb/...

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

.PHONY: build.dev.env
build.dev.env:
	docker build -t $(DOCKER_IMAGE) .
