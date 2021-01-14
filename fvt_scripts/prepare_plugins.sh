#!/bin/bash
set -e

go build -o fvt_scripts/plugins/pub/zmq_pub fvt_scripts/plugins/pub/zmq_pub.go
chmod +x fvt_scripts/plugins/pub/zmq_pub

go build -o fvt_scripts/plugins/service/http_server fvt_scripts/plugins/service/server.go
chmod +x fvt_scripts/plugins/service/http_server

cd fvt_scripts

rm -rf zmq.* Zmq.so

FILE=../plugins/sources/Zmq.so
if [ -f "$FILE" ]; then
    echo "$FILE exists, not requried to build plugin."
else
    echo "$FILE does not exist, will build the plugin."
    go build --buildmode=plugin -o ../plugins/sources/Zmq.so ../plugins/sources/zmq/zmq.go
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
    go build --buildmode=plugin -o ../plugins/functions/Image.so ../plugins/functions/image/*.go
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
