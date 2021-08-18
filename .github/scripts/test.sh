#!/bin/bash
#
# Copyright 2021 EMQ Technologies Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
