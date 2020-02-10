#!/bin/bash

ver=`git describe --tags --always`
os=`uname -s | tr "[A-Z]" "[a-z]"`
base_dir=_build/kuiper-"$ver"-"$os"-x86_64

rm -rf $base_dir/data/*
rm -rf $base_dir/log/*
touch $base_dir/log/kuiper.out
ls -l $base_dir/bin/server

echo "starting kuiper at " $base_dir
nohup $base_dir/bin/server > $base_dir/log/kuiper.out 2>&1 &