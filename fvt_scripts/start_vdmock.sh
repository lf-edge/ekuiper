#!/bin/bash
echo "starting edgex value descriptor mockup server."
go build -o fvt_scripts/edgex/valuedesc/vdmocker fvt_scripts/edgex/valuedesc/vd_server.go
go build -o fvt_scripts/edgex/pub fvt_scripts/edgex/pub.go
export BUILD_ID=dontKillMe
nohup fvt_scripts/edgex/vdmocker > vdmocker.out 2>&1 &