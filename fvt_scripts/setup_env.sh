#!/bin/bash

emqx_ids=`ps aux|grep "emqx" | grep "/usr/bin"|awk '{printf $2 " "}'`
if [ "$emqx_ids" = "" ] ; then
  echo "No emqx broker was started"
else
  for pid in $emqx_ids ; do
    echo "kill emqx: " $pid
    kill -9 $pid
  done
fi
echo "starting emqx..."
systemctl start emqx


pids=`ps aux|grep "server" | grep "bin"|awk '{printf $2 " "}'`
if [ "$pids" = "" ] ; then
   echo "No kuiper server was started"
else
  for pid in $pids ; do
    echo "kill kuiper " $pid
    kill -9 $pid
  done
fi

rm -rf data/*
rm -rf log/*
touch log/kuiper.out
pwd
ls -l log/kuiper.out
echo "starting kuiper"
nohup bin/server  > log/kuiper.out 2>&1 &