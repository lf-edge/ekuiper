#!/bin/sh
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
