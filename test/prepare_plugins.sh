#!/bin/bash
#
# Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

go build -o test/plugins/pub/zmq_pub test/plugins/pub/zmq_pub.go
chmod +x test/plugins/pub/zmq_pub

go build -o test/plugins/service/http_server test/plugins/service/server.go
chmod +x test/plugins/service/http_server

go build -o test/plugins/sql/create_table test/plugins/sql/create_table.go
chmod +x test/plugins/sql/create_table

cd test

rm -rf plugins/service/web/plugins/
mkdir -p plugins/service/web/plugins/

pids=`ps aux | grep http_server | grep "./" | awk '{printf $2 " "}'`
if [ "$pids" = "" ] ; then
   echo "No http mockup server was started"
else
  for pid in $pids ; do
    echo "kill http mockup server " $pid
    kill -9 $pid
  done
fi

cd plugins/service/
export BUILD_ID=dontKillMe
echo "starting mock http server..."
nohup ./http_server > http_server.out 2>&1 &
