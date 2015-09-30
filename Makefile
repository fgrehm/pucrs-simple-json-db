DOCKER_IMAGE := fgrehm/pucrs-metadata-db
DOCKER_DEV_CONTAINER_NAME := pucrs-metadata-db-dev

all: build

.PHONY: build
build: bin/metadata-db

bin/metadata-db: src/**/*.go
	gb build all

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
