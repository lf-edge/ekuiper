#!/bin/bash
#
# Copyright 2023 EMQ Technologies Co., Ltd.
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

set -euo pipefail

cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")"

PLUGIN_TYPE=$1
PLUGIN_NAME=$2
VERSION=$(git describe --tags --always)
OS=$(sed -n '/^ID=/p' /etc/os-release | sed -r 's/ID=(.*)/\1/g')

pre(){
    mkdir -p _plugins/$OS/$PLUGIN_TYPE
    if [  $(cat etc/$PLUGIN_TYPE/$PLUGIN_NAME.json | jq -r ".libs") != 'null' ]; then
        for lib in $(cat etc/$PLUGIN_TYPE/$PLUGIN_NAME.json | jq -r ".libs[]"); do
            go get $lib;
        done
    fi
}

post(){
    if [ -f "etc/$PLUGIN_TYPE/$PLUGIN_NAME.yaml" ]; then
        cp etc/$PLUGIN_TYPE/$PLUGIN_NAME.yaml extensions/$PLUGIN_TYPE/$PLUGIN_NAME;
    fi
    cd extensions/$PLUGIN_TYPE/$PLUGIN_NAME
    zip -r ${PLUGIN_NAME}_$(go env GOARCH).zip .
    cd -
    mv $(find extensions/$PLUGIN_TYPE/$PLUGIN_NAME -name "*.zip")  _plugins/$OS/$PLUGIN_TYPE
}

build(){
    case $PLUGIN_NAME in
        zmq )
          apt-get update
          apt-get install -y pkg-config libzmq-dev
          go build -trimpath --buildmode=plugin -o extensions/$PLUGIN_TYPE/$PLUGIN_NAME/$PLUGIN_NAME@$VERSION.so extensions/$PLUGIN_TYPE/$PLUGIN_NAME/*.go
          ;;
        influx )
            go build -trimpath --buildmode=plugin -tags plugins -o extensions/$PLUGIN_TYPE/$PLUGIN_NAME/$PLUGIN_NAME@$VERSION.so extensions/$PLUGIN_TYPE/$PLUGIN_NAME/$PLUGIN_NAME.go
            ;;
        tdengine )
            if [ "$(uname -m)" = "x86_64" ]; then
                wget "https://www.taosdata.com/assets-download/TDengine-client-2.4.0.18-Linux-x64.tar.gz" -O /tmp/TDengine-client-2.4.0.18.tar.gz;
            fi;
            if [ "$(uname -m)" = "aarch64" ]; then
                wget "https://www.taosdata.com/assets-download/TDengine-client-2.4.0.18-Linux-aarch64.tar.gz" -O /tmp/TDengine-client-2.4.0.18.tar.gz;
            fi;
            tar -zxvf /tmp/TDengine-client-2.4.0.18.tar.gz
            cd TDengine-client-2.4.0.18 && ./install_client.sh && cd -
            go build -trimpath --buildmode=plugin -tags plugins -o extensions/$PLUGIN_TYPE/$PLUGIN_NAME/$PLUGIN_NAME@$VERSION.so extensions/$PLUGIN_TYPE/$PLUGIN_NAME/$PLUGIN_NAME.go
            ;;
        labelImage )
            if [ ! -d "/tmp/tensorflow" ];then
                git clone -b v2.2.0-rc3 --depth 1 https://github.com/tensorflow/tensorflow.git /tmp/tensorflow;
            fi;
            if [ "$(uname -m)" = "x86_64" ]; then
                cp $(pwd)/extensions/functions/dependencies/tensorflow/amd64/*.so $(pwd)/extensions/functions/labelImage/lib
            fi;
            if [ "$(uname -m)" = "aarch64" ]; then
                cp $(pwd)/extensions/functions/dependencies/tensorflow/arm64/*.so $(pwd)/extensions/functions/labelImage/lib
            fi;
            CGO_CFLAGS=-I/tmp/tensorflow CGO_LDFLAGS=-L$(pwd)/extensions/functions/labelImage/lib go build -trimpath --buildmode=plugin -o extensions/functions/labelImage/labelImage@$VERSION.so extensions/functions/labelImage/*.go
            ;;
        tfLite )
            if [ ! -d "/tmp/tensorflow" ];then
                git clone -b v2.2.0-rc3 --depth 1 https://github.com/tensorflow/tensorflow.git /tmp/tensorflow;
            fi;
            if [ "$(uname -m)" = "x86_64" ]; then
                cp $(pwd)/extensions/functions/dependencies/tensorflow/amd64/*.so $(pwd)/extensions/functions/tfLite/lib
            fi;
            if [ "$(uname -m)" = "aarch64" ]; then
                cp $(pwd)/extensions/functions/dependencies/tensorflow/arm64/*.so $(pwd)/extensions/functions/tfLite/lib
            fi;
                CGO_CFLAGS=-I/tmp/tensorflow CGO_LDFLAGS=-L$(pwd)/extensions/functions/tfLite/lib go build -trimpath --buildmode=plugin -o extensions/functions/tfLite/tfLite@$VERSION.so extensions/functions/tfLite/*.go
            ;;
        * )
            go build -trimpath --buildmode=plugin -o extensions/$PLUGIN_TYPE/$PLUGIN_NAME/$PLUGIN_NAME@$VERSION.so extensions/$PLUGIN_TYPE/$PLUGIN_NAME/*.go
          ;;
    esac
}

pre
build
post
