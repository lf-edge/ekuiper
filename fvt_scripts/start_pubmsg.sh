#!/bin/bash

go build -o pubmsg fvt_scripts/edgex/pub.go
export BUILD_ID=dontKillMe
nohup ./pubmsg > log/pubmsg.out 2>&1 &