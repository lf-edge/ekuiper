#!/bin/sh
if [ ! -z "$DEBUG" ]; then
    set -ex
else
    set -e
fi

KUIPER_HOME=${KUIPER_HOME:-"/kuiper"}

/usr/bin/kuiper_conf_util

exec "$@"
