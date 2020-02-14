#!/bin/bash

function downloadjar
{
  if [ ! -f $1 ];then
    wget -O $1 $2
  else
    echo "Already downloaded $1."
  fi
}

downloadjar "/opt/jmeter/lib/json-lib-2.4-jdk15.jar" https://repo1.maven.org/maven2/net/sf/json-lib/json-lib/2.4/json-lib-2.4-jdk15.jar
downloadjar "/opt/jmeter/lib/commons-beanutils-1.8.0.jar" https://repo1.maven.org/maven2/commons-beanutils/commons-beanutils/1.8.0/commons-beanutils-1.8.0.jar
downloadjar "/opt/jmeter/lib/commons-collections-3.2.1.jar" https://repo1.maven.org/maven2/commons-collections/commons-collections/3.2.1/commons-collections-3.2.1.jar
downloadjar "/opt/jmeter/lib/commons-lang-2.5.jar" https://repo1.maven.org/maven2/commons-lang/commons-lang/2.5/commons-lang-2.5.jar
downloadjar "/opt/jmeter/lib/commons-logging-1.1.1.jar" https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar
downloadjar "/opt/jmeter/lib/ezmorph-1.0.6.jar" https://repo1.maven.org/maven2/net/sf/ezmorph/ezmorph/1.0.6/ezmorph-1.0.6.jar

ver=`git describe --tags --always`
os=`uname -s | tr "[A-Z]" "[a-z]"`
base_dir=_build/kuiper-"$ver"-"$os"-x86_64
fvt_dir=`pwd`

rm -rf *.jtl

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/streams_test.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l stream_test.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/rule_test.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l rule_test.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/select_all_rule.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l select_all_rule.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/select_condition_rule.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l select_condition_rule.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/select_aggr_rule.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l select_aggr_rule.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/change_rule_status.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l change_rule_status.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/change_stream_rule.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l change_stream_rule.jtl
echo -e "---------------------------------------------\n"