#!/bin/bash
set -e -x -u
export PKG_PATH=${PKG_PATH:-"_packages"}

if dpkg --help >/dev/null 2>&1; then
    dpkg -i $PKG_PATH/*.deb
    [ "$(dpkg -l |grep kuiper |awk '{print $1}')" = "ii" ]
    kuiperd &
    sleep 1
    if ! curl 127.0.0.1:9081  >/dev/null 2>&1; then echo "kuiper start failed"; exit 1; fi
    dpkg -r kuiper
    [ "$(dpkg -l |grep kuiper |awk '{print $1}')" = "rc" ]
    dpkg -P kuiper
    [ -z "$(dpkg -l |grep kuiper)" ]
fi

if rpm --help >/dev/null 2>&1; then
    rpm -ivh $PKG_PATH/*.rpm
    [ ! -z $(rpm -q emqx | grep -o emqx) ]
    kuiperd &
    sleep 1
    if ! curl 127.0.0.1:9081  >/dev/null 2>&1; then echo "kuiper start failed"; exit 1; fi
    rpm -e kuiper
    [ "$(rpm -q emqx)" == "package emqx is not installed" ]
fi
