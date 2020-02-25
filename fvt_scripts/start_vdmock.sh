#!/bin/bash



echo "starting edgex value descriptor mockup server."
go build -o vdmocker fvt_scripts/edgex/vd_server.go
export BUILD_ID=dontKillMe
nohup ./vdmocker > log/vdmocker.out 2>&1 &
