# Keep this short to avoid bloating beam files with long file path info
TOPDIR := /tmp/kuiper
SRCDIR := $(TOPDIR)/$(PKG_VSN)
BUILT := $(SRCDIR)/BUILT

TARGET_PKG := kuiper-$(PKG_VSN)-linux-$(shell go env GOARCH).deb
.PHONY: all
all: | $(BUILT)
	cp -r debian $(SRCDIR)/
	sed -i "s#<DATETIME>#$(shell date -u '+%a, %d %b %Y %T %z')#g" $(SRCDIR)/debian/changelog
	sed -i "s#<VERSION>#$(PKG_VSN)#g" $(SRCDIR)/debian/changelog
	cd $(SRCDIR) && dpkg-buildpackage -us -uc
	mkdir -p $(KUIPER_SOURCE)/_packages/
	cp $(SRCDIR)/../*.deb $(KUIPER_SOURCE)/_packages/$(TARGET_PKG)

$(BUILT):
	mkdir -p $(TOPDIR) $(SRCDIR)
	cp -r $(KUIPER_SOURCE)/. $(SRCDIR) 
	rm -rf $(SRCDIR)/plugins/source/*
	rm -rf $(SRCDIR)/plugins/sinks/*
	rm -rf $(SRCDIR)/plugins/functions/*
	rm -rf $(SRCDIR)/plugins/portable/*

clean:
	rm -rf $(TOPDIR)
