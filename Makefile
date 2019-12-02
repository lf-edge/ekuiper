BUILD_PATH ?= _build
PACKAGES_PATH ?= _packages

GO111MODULE ?= 
GOPROXY ?= https://goproxy.io

GOOS ?= ""
GOARCH ?= ""

VERSION := $(shell git describe --tags --always)
PACKAGE_NAME := kuiper-$(VERSION)
ifeq ($(GOOS), "")
	PACKAGE_NAME := $(PACKAGE_NAME)-$(shell uname -s | tr "[A-Z]" "[a-z]")
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
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/sources
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/etc/sinks
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/data
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/sources
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/sinks
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/plugins/functions
	@mkdir -p $(BUILD_PATH)/$(PACKAGE_NAME)/log

	@cp -r etc/* $(BUILD_PATH)/$(PACKAGE_NAME)/etc

	@if [ ! -z $(GOOS) ] && [ ! -z $(GOARCH) ];then \
		GO111MODULE=on GOPROXY=https://goproxy.io GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION)" -o cli xstream/cli/main.go; \
		GO111MODULE=on GOPROXY=https://goproxy.io GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=$(VERSION)" -o server xstream/server/main.go; \
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

.PHONY: clean
clean:
	rm -rf _build