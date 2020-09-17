BUILD_PATH ?= _build
PACKAGES_PATH ?= _packages

CGO_ENABLED ?= 1
GOOS ?= ""
GOARCH ?= ""

VERSION := $(shell git describe --tags --always)
OS := $(shell uname -s | tr "[A-Z]" "[a-z]")
PACKAGE_NAME := kuiper-$(VERSION)
ifeq ($(GOOS), "")
	PACKAGE_NAME := $(PACKAGE_NAME)-$(OS)
else
	PACKAGE_NAME := $(PACKAGE_NAME)-$(GOOS)
endif
ifeq ($(GOARCH), "")
	PACKAGE_NAME := $(PACKAGE_NAME)-$(shell uname -m)
else
	PACKAGE_NAME := $(PACKAGE_NAME)-$(GOARCH)
endif

TARGET ?= emqx/kuiper

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
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/data
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/sources
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/sinks
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/functions
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/log

	@cp -r etc/* $(BUILD_PATH)/$(PACKAGE_NAME)/etc

.PHONY: build_without_edgex
build_without_edgex: build_prepare
	@if [ ! -z $(GOOS) ] && [ ! -z $(GOARCH) ] && [ $(CGO_ENABLED) == 0 ];then \
		GO111MODULE=on GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o cli xstream/cli/main.go; \
		GO111MODULE=on GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o server xstream/server/main.go; \
	else \
		GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o cli xstream/cli/main.go; \
		GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o server xstream/server/main.go; \
	fi
	@if [ ! -z $$(which upx) ] && [ "$$(uname -m)" != "aarch64" ]; then upx ./cli; upx ./server; fi
	@mv ./cli ./server $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Build successfully"

.PHONY: pkg_without_edgex
pkg_without_edgex: build_without_edgex
	@make real_pkg

.PHONY: build_with_edgex
build_with_edgex: build_prepare
	@if [ ! -z $(GOOS) ] && [ ! -z $(GOARCH) ] && [ $(CGO_ENABLED) == 0 ];then \
		GO111MODULE=on GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -tags edgex -o cli xstream/cli/main.go; \
		GO111MODULE=on GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -tags edgex -o server xstream/server/main.go; \
	else \
		GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -tags edgex -o cli xstream/cli/main.go; \
		GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -tags edgex -o server xstream/server/main.go; \
	fi
	@if [ ! -z $$(which upx) ] && [ "$$(uname -m)" != "aarch64" ]; then upx ./cli; upx ./server; fi
	@mv ./cli ./server $(BUILD_PATH)/$(PACKAGE_NAME)/bin
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
	@docker run --rm --privileged multiarch/qemu-user-static --reset -p yes

.PHONY: cross_build
cross_build: cross_prepare
	@docker buildx build --no-cache \
	--platform=linux/amd64,linux/arm64,linux/arm/v7,linux/386,linux/ppc64le \
	-t cross_build \
	--output type=tar,dest=cross_build.tar \
	-f .ci/Dockerfile .

	@mkdir -p $(PACKAGES_PATH)
	@tar -xvf cross_build.tar --wildcards linux_amd64/go/kuiper/_packages/ \
		&& mv linux_amd64/go/kuiper/_packages/* $(PACKAGES_PATH)
	@tar -xvf cross_build.tar --wildcards linux_arm64/go/kuiper/_packages/ \
		&& mv linux_arm64/go/kuiper/_packages/* $(PACKAGES_PATH)
	@tar -xvf cross_build.tar --wildcards linux_arm_v7/go/kuiper/_packages/ \
		&& mv linux_arm_v7/go/kuiper/_packages/* $(PACKAGES_PATH)
	@tar -xvf cross_build.tar --wildcards linux_ppc64le/go/kuiper/_packages/ \
		&& mv linux_ppc64le/go/kuiper/_packages/* $(PACKAGES_PATH)
	@tar -xvf cross_build.tar --wildcards linux_386/go/kuiper/_packages/ \
		&& mv linux_386/go/kuiper/_packages/kuiper-$(VERSION)-$(OS)-x86_64.tar.gz $(PACKAGES_PATH)/kuiper-$(VERSION)-linux-386.tar.gz \
		&& mv linux_386/go/kuiper/_packages/kuiper-$(VERSION)-$(OS)-x86_64.zip $(PACKAGES_PATH)/kuiper-$(VERSION)-linux-386.zip \
		&& mv linux_386/go/kuiper/_packages/*.deb $(PACKAGES_PATH)

	@rm -f cross_build.tar
	@echo "Cross build success"

.PHONY: cross_build_for_rpm
cross_build_for_rpm: cross_prepare
	@docker buildx build --no-cache \
	--platform=linux/amd64,linux/arm64,linux/386,linux/ppc64le \
	-t cross_build \
	--output type=tar,dest=cross_build_for_rpm.tar \
	-f .ci/Dockerfile-centos .

	@mkdir -p $(PACKAGES_PATH)
	@tar -xvf cross_build_for_rpm.tar --wildcards linux_amd64/go/kuiper/_packages/ \
		&& mv linux_amd64/go/kuiper/_packages/*.rpm $(PACKAGES_PATH)
	@tar -xvf cross_build_for_rpm.tar --wildcards linux_arm64/go/kuiper/_packages/ \
		&& mv linux_arm64/go/kuiper/_packages/*.rpm $(PACKAGES_PATH)
	@tar -xvf cross_build_for_rpm.tar --wildcards linux_ppc64le/go/kuiper/_packages/ \
		&& mv linux_ppc64le/go/kuiper/_packages/*.rpm $(PACKAGES_PATH)
	@tar -xvf cross_build_for_rpm.tar --wildcards linux_386/go/kuiper/_packages/ \
		&& source_pkg=$$(basename linux_386/go/kuiper/_packages/*.rpm |head -1) \
		&& target_pkg=$$(echo $$source_pkg | sed 's/x86_64/386/g' ) \
		&& mv linux_386/go/kuiper/_packages/$$source_pkg $(PACKAGES_PATH)/$$target_pkg

	@rm -f cross_build_for_rpm.tar
	@echo "Cross build rpm packages success"


.PHONE: all_pkgs
all_pkgs: cross_build cross_build_for_rpm

.PHONY: docker
docker:
	docker buildx build --no-cache --platform=linux/amd64 -t $(TARGET):$(VERSION) -f deploy/docker/Dockerfile . --load
	docker buildx build --no-cache --platform=linux/amd64 -t $(TARGET):$(VERSION)-slim -f deploy/docker/Dockerfile-slim . --load
	docker buildx build --no-cache --platform=linux/amd64 -t $(TARGET):$(VERSION)-alpine -f deploy/docker/Dockerfile-alpine . --load

.PHONY:cross_docker
cross_docker: cross_prepare
	docker buildx build --no-cache \
	--platform=linux/amd64,linux/arm64,linux/arm/v7,linux/386,linux/ppc64le \
	-t $(TARGET):$(VERSION) \
	-f deploy/docker/Dockerfile . \
	--push

	docker buildx build --no-cache \
	--platform=linux/amd64,linux/arm64,linux/arm/v7,linux/386,linux/ppc64le \
	-t $(TARGET):$(VERSION)-slim \
	-f deploy/docker/Dockerfile-slim . \
	--push

	docker buildx build --no-cache \
	--platform=linux/amd64,linux/arm64,linux/arm/v7,linux/386,linux/ppc64le \
	-t $(TARGET):$(VERSION)-alpine \
	-f deploy/docker/Dockerfile-alpine . \
	--push


PLUGINS := sinks/file \
	sinks/influxdb \
	sinks/taos \
	sinks/zmq \
	sources/random \
	sources/zmq \
	functions/accumulateWordCount \
	functions/countPlusOne \
	functions/echo

.PHONE: plugins
plugins: cross_prepare $(PLUGINS)
$(PLUGINS): PLUGIN_TYPE = $(word 1, $(subst /, , $@))
$(PLUGINS): PLUGIN_NAME = $(word 2, $(subst /, , $@))
$(PLUGINS):
	@docker buildx build --no-cache \
    --platform=linux/amd64,linux/arm64,linux/arm/v7,linux/386,linux/ppc64le \
    -t cross_build \
    --build-arg VERSION=$(VERSION) \
    --build-arg PLUGIN_TYPE=$(PLUGIN_TYPE)\
    --build-arg PLUGIN_NAME=$(PLUGIN_NAME)\
    --output type=tar,dest=/tmp/cross_build_plugins_$(PLUGIN_TYPE)_$(PLUGIN_NAME).tar \
    -f .ci/Dockerfile-plugins .

	@mkdir -p _plugins/debian/$(PLUGIN_TYPE)
	@for arch in amd64 arm64 arm_v7 386 ppc64le; do \
		tar -xvf /tmp/cross_build_plugins_$(PLUGIN_TYPE)_$(PLUGIN_NAME).tar --wildcards "linux_$${arch}/go/kuiper/_plugins/$(PLUGIN_TYPE)/$(PLUGIN_NAME)/$(PLUGIN_NAME)_$$(echo $${arch%%_*}).zip" \
		&& mv $$(ls linux_$${arch}/go/kuiper/_plugins/$(PLUGIN_TYPE)/$(PLUGIN_NAME)/$(PLUGIN_NAME)_$$(echo $${arch%%_*}).zip) _plugins/debian/$(PLUGIN_TYPE); \
	done
	@rm -f /tmp/cross_build_plugins_$(PLUGIN_TYPE)_$(PLUGIN_NAME).tar

.PHONE: apline_plugins
alpine_plugins: cross_prepare $(PLUGINS:%=alpine/%)
$(PLUGINS:%=alpine/%): PLUGIN_TYPE = $(word 2, $(subst /, , $@))
$(PLUGINS:%=alpine/%): PLUGIN_NAME = $(word 3, $(subst /, , $@))
$(PLUGINS:%=alpine/%):
	@docker buildx build --no-cache \
    --platform=linux/amd64,linux/arm64,linux/arm/v7,linux/386,linux/ppc64le \
    -t cross_build \
    --build-arg VERSION=$(VERSION) \
    --build-arg PLUGIN_TYPE=$(PLUGIN_TYPE)\
    --build-arg PLUGIN_NAME=$(PLUGIN_NAME)\
    --output type=tar,dest=/tmp/cross_build_plugins_$(PLUGIN_TYPE)_$(PLUGIN_NAME)_on_alpine.tar \
    -f .ci/Dockerfile-plugins .

	@mkdir -p _plugins/alpine/$(PLUGIN_TYPE)
	@for arch in amd64 arm64 arm_v7 386 ppc64le; do \
		tar -xvf /tmp/cross_build_plugins_$(PLUGIN_TYPE)_$(PLUGIN_NAME)_on_alpine.tar --wildcards "linux_$${arch}/go/kuiper/_plugins/$(PLUGIN_TYPE)/$(PLUGIN_NAME)/$(PLUGIN_NAME)_$$(echo $${arch%%_*}).zip" \
		&& mv $$(ls linux_$${arch}/go/kuiper/_plugins/$(PLUGIN_TYPE)/$(PLUGIN_NAME)/$(PLUGIN_NAME)_$$(echo $${arch%%_*}).zip) _plugins/alpine/$(PLUGIN_TYPE); \
	done
	@rm -f /tmp/cross_build_plugins_$(PLUGIN_TYPE)_$(PLUGIN_NAME)_on_alpine.tar

.PHONY: clean
clean:
	@rm -rf cross_build.tar linux_amd64 linux_arm64 linux_arm_v7 linux_ppc64le linux_386
	@rm -rf _build _packages _plugins
