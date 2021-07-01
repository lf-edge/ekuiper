#!/bin/bash
set -e
echo "building edgex mock pubsub programs."

rm -rf test/edgex/pub
rm -rf test/edgex/sub/sub

go build -o test/edgex/pub test/edgex/pub.go
go build -o test/edgex/sub/sub test/edgex/sub/sub.go

chmod +x test/edgex/pub
chmod +x test/edgex/sub/sub

export BUILD_ID=dontKillMe