#!/bin/bash
set -e
echo "starting edgex value descriptor mockup server."

rm -rf fvt_scripts/edgex/valuedesc/vdmocker
rm -rf fvt_scripts/edgex/pub
rm -rf fvt_scripts/edgex/sub/sub

go build -o fvt_scripts/edgex/valuedesc/vdmocker fvt_scripts/edgex/valuedesc/vd_server.go
go build -o fvt_scripts/edgex/pub fvt_scripts/edgex/pub.go
go build -o fvt_scripts/edgex/sub/sub fvt_scripts/edgex/sub/sub.go

chmod +x fvt_scripts/edgex/valuedesc/vdmocker
chmod +x fvt_scripts/edgex/pub
chmod +x fvt_scripts/edgex/sub/sub

export BUILD_ID=dontKillMe
nohup fvt_scripts/edgex/valuedesc/vdmocker > vdmocker.out 2>&1 &