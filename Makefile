BUILD_PATH ?= _build
PACKAGES_PATH ?= _packages

GO111MODULE ?= 
GOPROXY ?= https://goproxy.io

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

.PHONY: build
build:
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/data
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/log

	@cp -r etc/* $(BUILD_PATH)/$(PACKAGE_NAME)/etc

	@if [ ! -z $(GOOS) ] && [ ! -z $(GOARCH) ] && [ $(CGO_ENABLED) == 0 ];then \
		GO111MODULE=on GOPROXY=https://goproxy.io GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w -X main.Version=$(VERSION)" -o cli xstream/cli/main.go; \
		GO111MODULE=on GOPROXY=https://goproxy.io GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w -X main.Version=$(VERSION)" -o server xstream/server/main.go; \
	else \
		GO111MODULE=on GOPROXY=https://goproxy.io CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION)" -o cli xstream/cli/main.go; \
		GO111MODULE=on GOPROXY=https://goproxy.io CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION)" -o server xstream/server/main.go; \
	fi
	@if [ ! -z $$(which upx) ]; then upx ./cli; upx ./server; fi
	@mv ./cli ./server $(BUILD_PATH)/$(PACKAGE_NAME)/bin
	@echo "Build successfully"

.PHONY: pkg
pkg: build
	@mkdir -p $(PACKAGES_PATH)
	@cd $(BUILD_PATH) && zip -rq $(PACKAGE_NAME).zip $(PACKAGE_NAME)
	@cd $(BUILD_PATH) && tar -czf $(PACKAGE_NAME).tar.gz $(PACKAGE_NAME)
	@mv $(BUILD_PATH)/$(PACKAGE_NAME).zip $(BUILD_PATH)/$(PACKAGE_NAME).tar.gz $(PACKAGES_PATH)
	@echo "Package build success"

.PHONY: cross_build
cross_build:
	@docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
	@docker buildx build --platform=linux/amd64,linux/arm64,linux/arm/v7,linux/386,linux/ppc64le \
	-t cross_build \
	--output type=tar,dest=cross_build.tar \
	-f ./Dockerfile-by-corss-build .

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

	@rm -rf cross_build.tar linux_amd64 linux_arm64 linux_arm_v7 linux_ppc64le linux_386
	@echo "Cross build success"

.PHONY: docker
docker:
	docker build -t emqx/kuiper:$(VERSION) -f .

.PHONY: clean
clean:
	@rm -rf _build _packages
