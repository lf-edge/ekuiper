BUILD_PATH ?= _build
PACKAGES_PATH ?= _packages

VERSION := $(shell git describe --tags --always)
ARCH := $(shell go env GOARCH)
OS := $(shell go env GOOS)
PACKAGE_NAME := kuiper-$(VERSION)-$(OS)-$(ARCH)
GO              := GO111MODULE=on go

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
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/wasm
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
	GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -tags "edgex include_nats_messaging" -o kuiper cmd/kuiper/main.go
	GO111MODULE=on CGO_ENABLED=1 go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -tags "edgex include_nats_messaging" -o kuiperd cmd/kuiperd/main.go
	@if [ ! -z $$(which upx) ]; then upx ./kuiper; upx ./kuiperd; fi
	@mv ./kuiper ./kuiperd $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Build successfully"

.PHONY: build_with_edgex_and_script
build_with_edgex_and_script: build_prepare
	GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -tags "edgex include_nats_messaging" -o kuiper cmd/kuiper/main.go
	GO111MODULE=on CGO_ENABLED=1 go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -tags "edgex include_nats_messaging script" -o kuiperd cmd/kuiperd/main.go
	@if [ ! -z $$(which upx) ]; then upx ./kuiper; upx ./kuiperd; fi
	@mv ./kuiper ./kuiperd $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Build successfully"

.PHONY: build_core
build_core: build_prepare
	GO111MODULE=on CGO_ENABLED=1 go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -tags core -o kuiperd cmd/kuiperd/main.go
	@if [ ! -z $$(which upx) ]; then upx ./kuiperd; fi
	@mv ./kuiperd $(BUILD_PATH)/$(PACKAGE_NAME)/bin
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

.PHONY: docker
docker:
	docker buildx build --no-cache --platform=linux/amd64 -t $(TARGET):$(VERSION) -f deploy/docker/Dockerfile . --load
	docker buildx build --no-cache --platform=linux/amd64 -t $(TARGET):$(VERSION)-slim -f deploy/docker/Dockerfile-slim . --load
	docker buildx build --no-cache --platform=linux/amd64 -t $(TARGET):$(VERSION)-dev -f deploy/docker/Dockerfile-dev . --load

PLUGINS := sinks/influx \
	sinks/influx2 \
	sinks/zmq \
	sinks/kafka \
	sinks/image \
	sinks/sql   \
	sources/random \
	sources/zmq \
	sources/sql \
	sources/video \
	sinks/tdengine \
	functions/accumulateWordCount \
	functions/countPlusOne \
	functions/image \
	functions/geohash \
	functions/echo \
	functions/labelImage \
	functions/tfLite

.PHONY: plugins $(PLUGINS)
plugins: $(PLUGINS)

$(PLUGINS): PLUGIN_TYPE = $(word 1, $(subst /, , $@))
$(PLUGINS): PLUGIN_NAME = $(word 2, $(subst /, , $@))
$(PLUGINS):
	@$(CURDIR)/build-plugins.sh $(PLUGIN_TYPE) $(PLUGIN_NAME)

.PHONY: clean
clean:
	@rm -rf cross_build.tar linux_amd64 linux_arm64 linux_arm_v7 linux_386
	@rm -rf _build _packages _plugins

tidy:
	@echo "go mod tidy"
	go mod tidy && git diff go.mod go.sum

lint:tools/check/bin/revive
	@echo "linting"
	@tools/check/bin/revive -formatter friendly -config tools/check/revive.toml ./...

tools/check/bin/revive:
	GOBIN=$(shell pwd)/tools/check/bin $(GO) install github.com/mgechev/revive@v1.2.1
