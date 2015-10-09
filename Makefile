DOCKER_IMAGE := fgrehm/pucrs-metadata-db
DOCKER_DEV_CONTAINER_NAME := pucrs-metadata-db-dev

all: build test

.PHONY: build
build: bin/metadata-db

bin/metadata-db: src/**/*.go
	gb build all

.PHONY: test
test:
	@echo 'Running tests...'
	gb test ./...

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
