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

set -e

go build -o test/plugins/pub/zmq_pub test/plugins/pub/zmq_pub.go
chmod +x test/plugins/pub/zmq_pub

go build -o test/plugins/service/http_server test/plugins/service/server.go
chmod +x test/plugins/service/http_server

cd test

rm -rf zmq.* Zmq.so

FILE=../plugins/sources/Zmq.so
if [ -f "$FILE" ]; then
    echo "$FILE exists, not requried to build plugin."
else
    echo "$FILE does not exist, will build the plugin."
    go build -trimpath -modfile ../extensions.mod --buildmode=plugin -o ../plugins/sources/Zmq.so ../extensions/sources/zmq/zmq.go
fi

mv ../plugins/sources/Zmq.so .
cp plugins/zmq.yaml .
zip zmq.zip Zmq.so zmq.yaml
rm -rf zmq.yaml Zmq.so

rm -rf image.* Image.so

FILE=../plugins/functions/Image.so
if [ -f "$FILE" ]; then
    echo "$FILE exists, not requried to build plugin."
else
    echo "$FILE does not exist, will build the plugin."
    go build -trimpath -modfile ../extensions.mod --buildmode=plugin -o ../plugins/functions/Image.so ../extensions/functions/image/*.go
fi

mv ../plugins/functions/Image.so .
zip image.zip Image.so
rm -rf Image.so

rm -rf plugins/service/web/plugins/
mkdir -p plugins/service/web/plugins/
mv zmq.zip plugins/service/web/plugins/
mv image.zip plugins/service/web/plugins/

cd plugins/service/
export BUILD_ID=dontKillMe

echo "starting mock http server..."
nohup ./http_server > http_server.out 2>&1 &
