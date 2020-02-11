#!/bin/bash

ver=`git describe --tags --always`
os=`uname -s | tr "[A-Z]" "[a-z]"`
base_dir=_build/kuiper-"$ver"-"$os"-x86_64

echo "Start to run stream script"
rm -rf stream_test.jtl
/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/streams_test.jmx -Dbase="$base_dir" -l stream_test.jtl
