# Copyright 2015 The Prometheus Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Needs to be defined before including Makefile.common to auto-generate targets
DOCKER_ARCHS ?= amd64 armv7 arm64

all: vet

include Makefile.common

STATICCHECK_IGNORE =

DOCKER_IMAGE_NAME ?= mysqld-exporter

.PHONY: crossbuild
crossbuild:
	@echo ">> cross-building binaries for multiple platforms"
	@mkdir -p .build/linux-amd64 .build/linux-arm64 .build/linux-armv7
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o .build/linux-amd64/mysqld_exporter .
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -ldflags="-s -w" -o .build/linux-arm64/mysqld_exporter .
	GOOS=linux GOARCH=arm CGO_ENABLED=0 GOARM=7 go build -ldflags="-s -w" -o .build/linux-armv7/mysqld_exporter .

.PHONY: docker-multiarch
docker-multiarch: crossbuild
	@echo ">> building and pushing multi-architecture Docker image"
	docker buildx create --use --name multi-arch-builder || true
	docker buildx build \
		--platform linux/amd64,linux/arm64,linux/arm/v7 \
		-t $(DOCKER_REPO)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG) \
		--push .

.PHONY: test-docker-single-exporter
test-docker-single-exporter:
	@echo ">> testing docker image for single exporter"
	./test_image.sh "$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)" 9104

.PHONY: test-docker
