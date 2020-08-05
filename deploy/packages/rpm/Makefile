# Keep this short to avoid bloating beam files with long file path info
TOPDIR := /tmp/kuiper
SRCDIR := $(TOPDIR)/$(PKG_VSN)
BUILT := $(SRCDIR)/BUILT
dash := -
none :=
space := $(none) $(none)
RPM_VSN ?= $(shell echo $(PKG_VSN) | grep -oE "[0-9]+\.[0-9]+(\.[0-9]+)?")
RPM_REL ?= $(shell echo $(PKG_VSN) | grep -oE "(alpha|beta|rc)\.[0-9]")

ifeq ($(RPM_REL),)
    # no tail
    RPM_REL := 1
endif

ARCH := $(shell uname -m)

ifeq ($(ARCH),mips64)
ARCH := mips64el
endif

TARGET_PKG := kuiper-$(PKG_VSN)-linux-$(ARCH).rpm

.PHONY: all
all: | $(BUILT)
	rpmbuild -v -bb \
		--define "_package_name kuiper" \
		--define "_name kuiper" \
		--define "_topdir $(TOPDIR)" \
		--define "_code_source $(SRCDIR)" \
		--define "_version $(RPM_VSN)" \
		--define "_release $(RPM_REL)" \
		--define "_service_src $(KUIPER_SOURCE)/deploy/packages/rpm/kuiper.service" \
		--define "_service_dst %{_unitdir}/kuiper.service" \
		--define "_post_addition %systemd_post %{_name}.service" \
		--define "_preun_addition %systemd_preun %{_name}.service" \
		--define "_sharedstatedir /var/lib" \
		kuiper.spec
	mkdir -p $(KUIPER_SOURCE)/_packages
	cp $(TOPDIR)/RPMS/$(ARCH)/*.rpm $(KUIPER_SOURCE)/_packages/$(TARGET_PKG)

$(BUILT):
	mkdir -p $(TOPDIR) $(SRCDIR)
	cp -r $(KUIPER_SOURCE)/. $(SRCDIR) 
	cd $(SRCDIR) && mkdir BUILT 

clean:
	rm -rf $(SRCDIR)
