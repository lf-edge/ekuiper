#!/bin/bash

emqx_ids=`ps aux|grep "emqx" | grep "/usr/bin"|awk '{printf $2 " "}'`
if [ "$emqx_ids" = "" ] ; then
  echo "No emqx broker was started"
  echo "starting emqx..."
  systemctl start emqx
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

ver=`git describe --tags --always`
os=`uname -s | tr "[A-Z]" "[a-z]"`
base_dir=_build/kuiper-"$ver"-"$os"-x86_64

rm -rf $base_dir/data/*
rm -rf $base_dir/log/*
touch $base_dir/log/kuiper.out

ls -l $base_dir/bin/server
echo "starting kuiper at " $base_dir
sh 'JENKINS_NODE_COOKIE=dontKillMe nohup $base_dir/bin/server > $base_dir/log/kuiper.out 2>&1 &'