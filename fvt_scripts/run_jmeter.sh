#!/bin/bash

ver=`git describe --tags --always`
os=`uname -s | tr "[A-Z]" "[a-z]"`
base_dir=_build/kuiper-"$ver"-"$os"-x86_64
fvt_dir=`pwd`

rm -rf *.jtl

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/streams_test.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l stream_test.jtl

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/rule_test.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l rule_test.jtl

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/simple_rule.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l simple_rule.jtl