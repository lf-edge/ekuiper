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

.PHONY: build
build: build_without_edgex

.PHONY:pkg
pkg: pkg_without_edgex

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
		GO111MODULE=on GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w -X main.Version=$(VERSION)" -o cli xstream/cli/main.go; \
		GO111MODULE=on GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w -X main.Version=$(VERSION)" -o server xstream/server/main.go; \
	else \
		GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION)" -o cli xstream/cli/main.go; \
		GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION)" -o server xstream/server/main.go; \
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
		GO111MODULE=on GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w -X main.Version=$(VERSION)" -tags edgex -o cli xstream/cli/main.go; \
		GO111MODULE=on GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w -X main.Version=$(VERSION)" -tags edgex -o server xstream/server/main.go; \
	else \
		GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION)" -tags edgex -o cli xstream/cli/main.go; \
		GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION)" -tags edgex -o server xstream/server/main.go; \
	fi
	@if [ ! -z $$(which upx) ] && [ "$$(uname -m)" != "aarch64" ]; then upx ./cli; upx ./server; fi
	@mv ./cli ./server $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Build successfully"

.PHONY: pkg_with_edgex
pkg_whit_edgex: build_with_edgex 
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
	-f ./Dockerfile .

	@mkdir -p $(PACKAGES_PATH)
	@tar -xvf cross_build.tar --wildcards linux_amd64/go/kuiper/_packages/* \
		&& mv linux_amd64/go/kuiper/_packages/* $(PACKAGES_PATH)
	@tar -xvf cross_build.tar --wildcards linux_arm64/go/kuiper/_packages/* \
		&& mv linux_arm64/go/kuiper/_packages/* $(PACKAGES_PATH)
	@tar -xvf cross_build.tar --wildcards linux_arm_v7/go/kuiper/_packages/* \
		&& mv linux_arm_v7/go/kuiper/_packages/* $(PACKAGES_PATH)
	@tar -xvf cross_build.tar --wildcards linux_ppc64le/go/kuiper/_packages/* \
		&& mv linux_ppc64le/go/kuiper/_packages/* $(PACKAGES_PATH)
	@tar -xvf cross_build.tar --wildcards linux_386/go/kuiper/_packages/* \
		&& mv linux_386/go/kuiper/_packages/kuiper-$(VERSION)-$(OS)-x86_64.tar.gz $(PACKAGES_PATH)/kuiper-$(VERSION)-linux-386.tar.gz \
		&& mv linux_386/go/kuiper/_packages/kuiper-$(VERSION)-$(OS)-x86_64.zip $(PACKAGES_PATH)/kuiper-$(VERSION)-linux-386.zip

	@echo "Cross build success"

.PHONY: docker
docker:
	docker build --no-cache -t $(TARGET):$(VERSION) -f deploy/docker/Dockerfile .
	docker build --no-cache -t $(TARGET):$(VERSION)-slim -f deploy/docker/Dockerfile-slim .
	docker build --no-cache -t $(TARGET):$(VERSION)-alpine -f deploy/docker/Dockerfile-alpine .

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

.PHONY: clean
clean:
	@rm -rf cross_build.tar linux_amd64 linux_arm64 linux_arm_v7 linux_ppc64le linux_386
	@rm -rf _build _packages 
