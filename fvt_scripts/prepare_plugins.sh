#!/bin/bash
set -e

go build -o fvt_scripts/plugins/pub/zmq_pub fvt_scripts/plugins/pub/zmq_pub.go
chmod +x fvt_scripts/plugins/pub/zmq_pub

go build -o fvt_scripts/plugins/service/http_server fvt_scripts/plugins/service/server.go
chmod +x fvt_scripts/plugins/service/http_server

cd fvt_scripts

rm -rf zmq.* Zmq.so
go build --buildmode=plugin -o ../plugins/sources/Zmq.so ../plugins/sources/zmq.go

mv ../plugins/sources/Zmq.so .
cp plugins/zmq.yaml .
zip zmq.zip Zmq.so zmq.yaml
rm -rf zmq.yaml Zmq.so

rm -rf plugins/service/web/plugins/*
mv zmq.zip plugins/service/web/plugins/

cd plugins/service/
export BUILD_ID=dontKillMe

nohup ./http_server > http_server.out 2>&1 &