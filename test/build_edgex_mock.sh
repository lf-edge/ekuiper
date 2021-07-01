#!/bin/bash
set -e
echo "building edgex mock pubsub programs."

rm -rf fvt_scripts/edgex/pub
rm -rf fvt_scripts/edgex/sub/sub

go build -o fvt_scripts/edgex/pub fvt_scripts/edgex/pub.go
go build -o fvt_scripts/edgex/sub/sub fvt_scripts/edgex/sub/sub.go

chmod +x fvt_scripts/edgex/pub
chmod +x fvt_scripts/edgex/sub/sub

export BUILD_ID=dontKillMe