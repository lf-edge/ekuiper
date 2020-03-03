#!/bin/bash
# This script accepts the following parameters:
#
# * with_edgex
#
# Example:
#
# ./fvt_scripts/run_jmeter.sh with_edgex=true
#
# or
#
# ./fvt_scripts/run_jmeter.sh with_edgex=false
#

set -e

CONFIG=$@

for line in $CONFIG; do
  eval "$line"
done

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

rm -rf jmeter_logs

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/streams_test.jmx -Dbase="$base_dir" -l jmeter_logs/stream_test.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/rule_test.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l jmeter_logs/rule_test.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/select_all_rule.jmx -l jmeter_logs/select_all_rule.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/select_condition_rule.jmx -l jmeter_logs/select_condition_rule.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/select_aggr_rule.jmx -l jmeter_logs/select_aggr_rule.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/change_rule_status.jmx -l jmeter_logs/change_rule_status.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/change_stream_rule.jmx -l jmeter_logs/change_stream_rule.jtl
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/select_aggr_rule_order.jmx -l jmeter_logs/select_aggr_rule_order.jtl
echo -e "---------------------------------------------\n"

if test $with_edgex = true; then
  /opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t fvt_scripts/select_edgex_condition_rule.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l jmeter_logs/select_edgex_condition_rule.jtl
  echo -e "---------------------------------------------\n"
fi