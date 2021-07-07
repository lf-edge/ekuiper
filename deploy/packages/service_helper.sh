#!/bin/sh
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

set -e -u

case $1 in
    start)
        nohup /usr/bin/kuiperd >> /var/log/kuiper/nohup.out &
        ;;
    stop)
        pid=$(ps -ef |grep kuiperd |grep -v "grep" | awk '{print $2}')
        while $(kill "$pid" 2>/dev/null); do
            sleep 1
        done
        ;;
    ping)
        if [ "$(curl -sl -w %{http_code} 127.0.0.1:9081 -o /dev/null)" = "200" ]; then
            echo pong
        else
            echo "Ping kuiper failed"
            exit 1
        fi
        ;;
    *)
        echo "Usage: $SCRIPTNAME {start|stop|ping|restart|force-reload|status}" >&2
        exit 3
        ;;
esac
