BUILD_PATH ?= _build
PACKAGES_PATH ?= _packages

VERSION := $(shell git describe --tags --always)
PACKAGE_NAME := kuiper-$(VERSION)-$(shell go env GOOS)-$(shell go env GOARCH)

TARGET ?= lfedge/ekuiper

export KUIPER_SOURCE := $(shell pwd)

.PHONY: build
build: build_without_edgex

.PHONY:pkg
pkg: pkg_without_edgex
	@if [ "$$(uname -s)" = "Linux" ]; then make -C deploy/packages; fi

.PHONY: build_prepare
build_prepare:
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/sources
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/sinks
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/services
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/services/schemas
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/data
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/sources
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/sinks
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/functions
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/portable
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/log

	@cp -r etc/* $(BUILD_PATH)/$(PACKAGE_NAME)/etc

.PHONY: build_without_edgex
build_without_edgex: build_prepare
	GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o kuiper cmd/kuiper/main.go
	GO111MODULE=on CGO_ENABLED=1 go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o kuiperd cmd/kuiperd/main.go
	@if [ ! -z $$(which upx) ]; then upx ./kuiper; upx ./kuiperd; fi
	@mv ./kuiper ./kuiperd $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Build successfully"

.PHONY: pkg_without_edgex
pkg_without_edgex: build_without_edgex
	@make real_pkg

.PHONY: build_with_edgex
build_with_edgex: build_prepare
	GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -tags edgex -o kuiper cmd/kuiper/main.go
	GO111MODULE=on CGO_ENABLED=1 go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -tags edgex -o kuiperd cmd/kuiperd/main.go
	@if [ ! -z $$(which upx) ]; then upx ./kuiper; upx ./kuiperd; fi
	@mv ./kuiper ./kuiperd $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Build successfully"

.PHONY: pkg_with_edgex
pkg_with_edgex: build_with_edgex
	@make real_pkg

.PHONY: real_pkg
real_pkg:
	@mkdir -p $(PACKAGES_PATH)
	@cd $(BUILD_PATH) && zip -rq $(PACKAGE_NAME).zip $(PACKAGE_NAME)
	@cd $(BUILD_PATH) && tar -czf $(PACKAGE_NAME).tar.gz $(PACKAGE_NAME)
	@mv $(BUILD_PATH)/$(PACKAGE_NAME).zip $(BUILD_PATH)/$(PACKAGE_NAME).tar.gz $(PACKAGES_PATH)
	@echo "Package build success"

.PHONY:cross_prepare
cross_prepare:
	@docker run --rm --privileged tonistiigi/binfmt --install all

.PHONY: docker
docker:
	docker buildx build --no-cache --platform=linux/amd64 -t $(TARGET):$(VERSION) -f deploy/docker/Dockerfile . --load
	docker buildx build --no-cache --platform=linux/amd64 -t $(TARGET):$(VERSION)-slim -f deploy/docker/Dockerfile-slim . --load
	docker buildx build --no-cache --platform=linux/amd64 -t $(TARGET):$(VERSION)-alpine -f deploy/docker/Dockerfile-alpine . --load

.PHONY:cross_docker
cross_docker: cross_prepare
	docker buildx build --no-cache \
	--platform=linux/amd64,linux/arm64,linux/arm/v7,linux/386 \
	-t $(TARGET):$(VERSION) \
	-f deploy/docker/Dockerfile . \
	--push

	docker buildx build --no-cache \
	--platform=linux/amd64,linux/arm64,linux/arm/v7,linux/386 \
	-t $(TARGET):$(VERSION)-slim \
	-f deploy/docker/Dockerfile-slim . \
	--push

	docker buildx build --no-cache \
	--platform=linux/amd64,linux/arm64,linux/arm/v7,linux/386 \
	-t $(TARGET):$(VERSION)-alpine \
	-f deploy/docker/Dockerfile-alpine . \
	--push

PLUGINS := sinks/file \
	sinks/influx \
	sinks/zmq \
	sinks/image \
	sinks/redis \
	sources/random \
	sources/zmq \
	functions/accumulateWordCount \
	functions/countPlusOne \
	functions/image \
	functions/geohash \
	functions/echo

.PHONY: plugins sinks/tdengine $(PLUGINS)
plugins: cross_prepare sinks/tdengine functions/labelImage $(PLUGINS)
sinks/tdengine:
	@docker buildx build --no-cache \
    --platform=linux/amd64,linux/arm64 \
    -t cross_build \
    --build-arg VERSION=$(VERSION) \
    --build-arg PLUGIN_TYPE=sinks \
    --build-arg PLUGIN_NAME=tdengine \
    --output type=tar,dest=/tmp/cross_build_plugins_sinks_tdengine.tar \
    -f .ci/Dockerfile-plugins .

	@mkdir -p _plugins/debian/sinks
	@for arch in amd64 arm64; do \
		tar -xvf /tmp/cross_build_plugins_sinks_tdengine.tar --wildcards "linux_$${arch}/go/kuiper/plugins/sinks/tdengine/tdengine_$$(echo $${arch%%_*}).zip" \
		&& mv $$(ls linux_$${arch}/go/kuiper/plugins/sinks/tdengine/tdengine_$$(echo $${arch%%_*}).zip) _plugins/debian/sinks; \
	done
	@rm -f /tmp/cross_build_plugins_sinks_tdengine.tar

functions/labelImage:
	@docker buildx build --no-cache \
    --platform=linux/amd64 \
    -t cross_build \
    --build-arg VERSION=$(VERSION) \
    --build-arg PLUGIN_TYPE=functions \
    --build-arg PLUGIN_NAME=labelImage \
    --output type=tar,dest=/tmp/cross_build_plugins_functions_labelImage.tar \
    -f .ci/Dockerfile-plugins .

	@mkdir -p _plugins/debian/functions
	@tar -xvf /tmp/cross_build_plugins_functions_labelImage.tar --wildcards "go/kuiper/plugins/functions/labelImage/labelImage_amd64.zip"
	@mv $$(ls go/kuiper/plugins/functions/labelImage/labelImage_amd64.zip) _plugins/debian/functions
	@rm -f /tmp/cross_build_plugins_functions_labelImage.tar

$(PLUGINS): PLUGIN_TYPE = $(word 1, $(subst /, , $@))
$(PLUGINS): PLUGIN_NAME = $(word 2, $(subst /, , $@))
$(PLUGINS):
	@docker buildx build --no-cache \
    --platform=linux/amd64,linux/arm64,linux/arm/v7,linux/386 \
    -t cross_build \
    --build-arg VERSION=$(VERSION) \
    --build-arg PLUGIN_TYPE=$(PLUGIN_TYPE)\
    --build-arg PLUGIN_NAME=$(PLUGIN_NAME)\
    --output type=tar,dest=/tmp/cross_build_plugins_$(PLUGIN_TYPE)_$(PLUGIN_NAME).tar \
    -f .ci/Dockerfile-plugins .

	@mkdir -p _plugins/debian/$(PLUGIN_TYPE)
	@for arch in amd64 arm64 arm_v7 386; do \
		tar -xvf /tmp/cross_build_plugins_$(PLUGIN_TYPE)_$(PLUGIN_NAME).tar --wildcards "linux_$${arch}/go/kuiper/plugins/$(PLUGIN_TYPE)/$(PLUGIN_NAME)/$(PLUGIN_NAME)_$$(echo $${arch%%_*}).zip" \
		&& mv $$(ls linux_$${arch}/go/kuiper/plugins/$(PLUGIN_TYPE)/$(PLUGIN_NAME)/$(PLUGIN_NAME)_$$(echo $${arch%%_*}).zip) _plugins/debian/$(PLUGIN_TYPE); \
	done
	@rm -f /tmp/cross_build_plugins_$(PLUGIN_TYPE)_$(PLUGIN_NAME).tar

.PHONY: clean
clean:
	@rm -rf cross_build.tar linux_amd64 linux_arm64 linux_arm_v7 linux_386
	@rm -rf _build _packages _plugins
