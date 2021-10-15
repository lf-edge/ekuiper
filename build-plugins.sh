#!/bin/bash
set -euo pipefail

PLUGIN_TYPE=$1
PLUGIN_NAME=$2

cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")"

pre(){
    mkdir -p _plugins/debian/$PLUGIN_TYPE/$PLUGIN_NAME
    for lib in $(cat etc/$PLUGIN_TYPE/$PLUGIN_NAME.json | jq -r ".libs[]"); do 
        go get $lib; 
    done
}
    
post(){
    if [ -f "etc/$PLUGIN_TYPE/$PLUGIN_NAME.yaml" ]; then 
        cp etc/$PLUGIN_TYPE/$PLUGIN_NAME.yaml _plugins/debian/$PLUGIN_TYPE/$PLUGIN_NAME; 
    fi
    cd extensions/$PLUGIN_TYPE/$PLUGIN_NAME
    zip -r ${PLUGIN_NAME}_$(go env GOARCH).zip .
    cd -
    mv $(find extensions/$PLUGIN_TYPE/$PLUGIN_NAME -name "*.zip")  _plugins/debian/$PLUGIN_TYPE/$PLUGIN_NAME
}

build(){
case $PLUGIN_NAME in
    influxdb )
        go build -trimpath -modfile extensions.mod --buildmode=plugin -tags plugins -o extensions/$PLUGIN_TYPE/$PLUGIN_NAME/$PLUGIN_NAME@$VERSION.so extensions/$PLUGIN_TYPE/$PLUGIN_NAME/$PLUGIN_NAME.go
        ;;
    tdengine )
        if [ "$(uname -m)" = "x86_64" ]; then
            wget "https://www.taosdata.com/assets-download/TDengine-client-2.2.0.5-Linux-x64.tar.gz" -O /tmp/TDengine-client-2.2.0.5.tar.gz;
        fi;
        if [ "$(uname -m)" = "aarch64" ]; then
            wget "https://www.taosdata.com/assets-download/TDengine-client-2.2.0.5-Linux-aarch64.tar.gz" -O /tmp/TDengine-client-2.2.0.5.tar.gz;
        fi;
        tar -zxvf /tmp/TDengine-client-2.2.0.5.tar.gz
        cd TDengine-client-2.2.0.5 && ./install_client.sh && cd -
        go build -trimpath -modfile extensions.mod --buildmode=plugin -tags plugins -o extensions/$PLUGIN_TYPE/$PLUGIN_NAME/$PLUGIN_NAME@$VERSION.so extensions/$PLUGIN_TYPE/$PLUGIN_NAME/$PLUGIN_NAME.go
        ;;
    labelImage )
        git clone -b v2.2.0-rc3 --depth 1 https://github.com/tensorflow/tensorflow.git /tmp/tensorflow;
        CGO_CFLAGS=-I/tmp/tensorflow CGO_LDFLAGS=-Lextensions/functions/labelImage/lib go build -trimpath -modfile extensions.mod --buildmode=plugin -o extensions/functions/labelImage/labelImage.so extensions/functions/labelImage/*.go
        ;;
    * )
        go build -trimpath -modfile extensions.mod --buildmode=plugin -o extensions/$PLUGIN_TYPE/$PLUGIN_NAME/$PLUGIN_NAME@$VERSION.so extensions/$PLUGIN_TYPE/$PLUGIN_NAME/*.go
      ;;
esac
}

pre
build
post
