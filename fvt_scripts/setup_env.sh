#!/bin/bash

emqx_ids=`ps aux|grep "emqx" | grep "/usr/bin"|awk '{printf $2 " "}'`
if [ "$emqx_ids" = "" ] ; then
  echo "No emqx broker was started"
  echo "starting emqx..."
  emqx start
else
  echo "emqx has already started"
  #for pid in $emqx_ids ; do
    #echo "kill emqx: " $pid
    #kill -9 $pid
  #done
fi

pids=`ps aux|grep "server" | grep "bin"|awk '{printf $2 " "}'`
if [ "$pids" = "" ] ; then
   echo "No kuiper server was started"
else
  for pid in $pids ; do
    echo "kill kuiper " $pid
    kill -9 $pid
  done
fi

fvt_scripts/start_kuiper.sh