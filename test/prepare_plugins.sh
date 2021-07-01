#!/bin/bash
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
