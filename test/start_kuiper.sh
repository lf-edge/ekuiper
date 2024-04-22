#!/bin/bash
#
# Copyright 2021 EMQ Technologies Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

pids=`ps aux|grep "kuiperd" | grep "bin"|awk '{printf $2 " "}'`
if [ "$pids" = "" ] ; then
   echo "No kuiper server was started"
else
  for pid in $pids ; do
    echo "kill kuiper " $pid
    kill -9 $pid
  done
fi

ver=`git describe --tags --always | sed 's/^v//g'`
os=`uname -s | tr "[A-Z]" "[a-z]"`
base_dir=_build/kuiper-"$ver"-"$os"-amd64

rm -rf $base_dir/data/*
ls -l $base_dir/bin/kuiperd

cd $base_dir/
touch log/kuiper.out
export BUILD_ID=dontKillMe
export KUIPER__BASIC__PROMETHEUS="true"
export KUIPER__BASIC__PROMETHEUSPORT=9081
export KUIPER__BASIC__RESTPORT=9081
export KUIPER__PORTABLE__INITTIMEOUT=50000
nohup bin/kuiperd > log/kuiper.out 2>&1 &
echo "starting kuiper at " $base_dir

