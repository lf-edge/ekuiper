#!/bin/bash
set -e

ver=`git describe --tags --always`
os=`uname -s | tr "[A-Z]" "[a-z]"`
base_dir=_build/kuiper-"$ver"-"$os"-x86_64

rm -rf $base_dir/data/*
ls -l $base_dir/bin/kuiperd

cd $base_dir/
touch log/kuiper.out
export BUILD_ID=dontKillMe
nohup bin/kuiperd > log/kuiper.out 2>&1 &
echo "starting kuiper at " $base_dir

