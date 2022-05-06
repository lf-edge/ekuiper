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

# build tdengine plugins
FILE=../plugins/sinks/Tdengine.so
if [ -f "$FILE" ]; then
    echo "$FILE exists, not requried to build plugin."
else
    echo "$FILE does not exist, will build the plugin."
    go build -trimpath -modfile ../extensions.mod --buildmode=plugin -o ../plugins/sinks/Tdengine.so ../extensions/sinks/tdengine/*.go
fi

mv ../plugins/sinks/Tdengine.so .
zip tdengine.zip Tdengine.so
rm -rf Tdengine.so

# build sql plugins
FILE=../plugins/sinks/Sql.so
if [ -f "$FILE" ]; then
    echo "$FILE exists, not requried to build plugin."
else
    echo "$FILE does not exist, will build the plugin."
    go build -trimpath -modfile ../extensions.mod --buildmode=plugin -o ../plugins/sinks/Sql.so ../extensions/sinks/sql/*.go
fi

mv ../plugins/sinks/Sql.so .
zip sql.zip Sql.so
rm -rf Sql.so

rm -rf plugins/service/web/plugins/
mkdir -p plugins/service/web/plugins/
mv zmq.zip plugins/service/web/plugins/
mv image.zip plugins/service/web/plugins/
mv tdengine.zip plugins/service/web/plugins/
mv sql.zip plugins/service/web/plugins/

# prepare portable plugins
cd ..
mkdir test/temp

mkdir test/temp/mirror
cd sdk/go/example/mirror
go build -o ../../../../test/temp/mirror/mirror .
cd ../../../..
cp sdk/go/example/mirror/mirror.json test/temp/mirror
cp -r sdk/go/example/mirror/sources test/temp/mirror/
cd test/temp/mirror
zip -r ../../plugins/service/web/plugins/mirror.zip *
cd ../../..

cp -r sdk/python/example/pysam test/temp/pysam
cp -r sdk/python/ekuiper test/temp/pysam/
cd test/temp/pysam
zip -r ../../plugins/service/web/plugins/pysam.zip *
cd ../..

rm -r temp

pids=`ps aux | grep http_server | grep "./" | awk '{printf $2 " "}'`
if [ "$pids" = "" ] ; then
   echo "No http mockup server was started"
else
  for pid in $pids ; do
    echo "kill http mockup server " $pid
    kill -9 $pid
  done
fi

cd plugins/service/
export BUILD_ID=dontKillMe
echo "starting mock http server..."
nohup ./http_server > http_server.out 2>&1 &
