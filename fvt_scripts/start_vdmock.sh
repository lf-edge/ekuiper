#!/bin/bash



echo "starting edgex value descriptor mockup server."
go build -o fvt_scripts/edgex/vdmocker fvt_scripts/edgex/vd_server.go
go build -o fvt_scripts/edgex/pub fvt_scripts/edgex/pub.go
export BUILD_ID=dontKillMe
nohup fvt_scripts/edgex/vdmocker > log/vdmocker.out 2>&1 &