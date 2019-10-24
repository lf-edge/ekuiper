BUILD_PATH ?= _build
PACKAGES_PATH ?= _packages

GO111MODULE ?= 
GOPROXY ?= https://goproxy.io

GOOS ?= ""
GOARCH ?= ""

.PHONY: build
build:
	@mkdir -p $(BUILD_PATH)/engine/bin
	@mkdir -p $(BUILD_PATH)/engine/etc
	@mkdir -p $(BUILD_PATH)/engine/data
	@mkdir -p $(BUILD_PATH)/engine/plugins
	@mkdir -p $(BUILD_PATH)/engine/log

	@cp -r etc/* $(BUILD_PATH)/engine/etc

	@if [ ! -z $(GOOS) ] && [ ! -z $(GOARCH) ];then \
		GO111MODULE=on GOPROXY=https://goproxy.io GOOS=$(GOOS) $(GOARCH)=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w" -o cli xstream/cli/main.go; \
		GO111MODULE=on GOPROXY=https://goproxy.io GOOS=$(GOOS) $(GOARCH)=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w" -o server xstream/server/main.go; \
	else \
		GO111MODULE=on GOPROXY=https://goproxy.io CGO_ENABLED=0 go build -ldflags="-s -w" -o cli xstream/cli/main.go; \
		GO111MODULE=on GOPROXY=https://goproxy.io CGO_ENABLED=0 go build -ldflags="-s -w" -o server xstream/server/main.go; \
	fi
	@if [ ! -z $$(which upx) ]; then upx ./cli; upx ./server; fi
	@mv ./cli ./server $(BUILD_PATH)/engine/bin
	@echo "Build successfully"

.PHONY: pkg
pkg: build
	@mkdir -p $(PACKAGES_PATH)
	@if [ ! -z $(GOOS) ] && [ ! -z $(GOARCH) ];then \
		package_name=engine_$(GOARCH); \
	else \
		package_name=engine; \
	fi; \
	cd $(BUILD_PATH); \
	zip -rq $${package_name}.zip engine; \
	tar -czf $${package_name}.tar.gz engine; \
	mv $${package_name}.zip $${package_name}.tar.gz ../$(PACKAGES_PATH)
	@echo "Package build success"

.PHONY: clean
clean:
	rm -rf _build