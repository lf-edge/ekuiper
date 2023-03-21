#!/bin/bash
#
# Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

# This script accepts the following parameters:
#
# * with_edgex
#
# Example:
#
# ./test/run_jmeter.sh with_edgex=true
#
# or
#
# ./test/run_jmeter.sh with_edgex=false
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
base_dir=_build/kuiper-"$ver"-"$os"-amd64
fvt_dir=`pwd`

rm -rf jmeter_logs

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/streams_test.jmx -Dbase="$base_dir" -l jmeter_logs/stream_test.jtl -j jmeter_logs/stream_test.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/rule_test.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l jmeter_logs/rule_test.jtl -j jmeter_logs/rule_test.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/select_all_rule.jmx -l jmeter_logs/select_all_rule.jtl -j jmeter_logs/select_all_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/select_condition_rule.jmx -l jmeter_logs/select_condition_rule.jtl -j jmeter_logs/select_condition_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/select_aggr_rule.jmx -l jmeter_logs/select_aggr_rule.jtl -j jmeter_logs/select_aggr_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/change_rule_status.jmx -l jmeter_logs/change_rule_status.jtl -j jmeter_logs/change_rule_status.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/change_stream_rule.jmx -l jmeter_logs/change_stream_rule.jtl -j jmeter_logs/change_stream_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/select_aggr_rule_order.jmx -l jmeter_logs/select_aggr_rule_order.jtl -j jmeter_logs/select_aggr_rule_order.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/rule_pipeline.jmx -l jmeter_logs/rule_pipeline.jtl -j jmeter_logs/rule_pipeline.log
echo -e "---------------------------------------------\n"

if test $with_edgex = true; then
  /opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/select_edgex_condition_rule.jmx -Dbase="$base_dir" -Dfvt="$fvt_dir" -l jmeter_logs/select_edgex_condition_rule.jtl -j jmeter_logs/select_edgex_condition_rule.log
  echo -e "---------------------------------------------\n"

  /opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/select_edgex_another_bus_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/select_edgex_another_bus_rule.jtl -j jmeter_logs/select_edgex_another_bus_rule.log
  echo -e "---------------------------------------------\n"

  /opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/edgex_sink_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/edgex_sink_rule.jtl -j jmeter_logs/edgex_sink_rule.log
  echo -e "---------------------------------------------\n"
  
  /opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/select_edgex_meta_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/select_edgex_meta_rule.jtl -j jmeter_logs/select_edgex_meta_rule.log
  echo -e "---------------------------------------------\n"

  /opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/edgex_mqtt_sink_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/edgex_mqtt_sink_rule.jtl -j jmeter_logs/edgex_mqtt_sink_rule.log
  echo -e "---------------------------------------------\n"

   /opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/edgex_redis_share_connection_sink_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/edgex_redis_share_connection_sink_rule.jtl -j jmeter_logs/edgex_redis_share_connection_sink_rule.log
  echo -e "---------------------------------------------\n"


  /opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/edgex_array_rule.jmx  -Dfvt="$fvt_dir" -l jmeter_logs/edgex_array_rule.jtl -j jmeter_logs/edgex_array_rule.log
  echo -e "---------------------------------------------\n"
fi

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/plugin_end_2_end.jmx -Dfvt="$fvt_dir" -l jmeter_logs/plugin_end_2_end.jtl -j jmeter_logs/plugin_end_2_end.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/portable_end_2_end.jmx -Dfvt="$fvt_dir" -l jmeter_logs/portable_end_2_end.jtl -j jmeter_logs/portable_end_2_end.log
echo -e "---------------------------------------------\n"


/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/select_countwindow_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/select_countwindow_rule.jtl -j jmeter_logs/select_countwindow_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/http_pull_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/http_pull_rule.jtl -j jmeter_logs/http_pull_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/binary_image_process.jmx -Dfvt="$fvt_dir" -l jmeter_logs/binary_image_process.jtl -j jmeter_logs/binary_image_process.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/table_static.jmx -Dfvt="$fvt_dir" -l jmeter_logs/table_static.jtl -j jmeter_logs/table_static.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/table_cont.jmx -Dfvt="$fvt_dir" -l jmeter_logs/table_cont.jtl -j jmeter_logs/table_cont.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/shared_source_rules.jmx -Dfvt="$fvt_dir" -l jmeter_logs/shared_source_rules.jtl -j jmeter_logs/shared_source_rules.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/graph_condition_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/graph_condition_rule.jtl -j jmeter_logs/graph_condition_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/graph_group_order_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/graph_group_order_rule.jtl -j jmeter_logs/graph_group_order_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/graph_group_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/graph_group_rule.jtl -j jmeter_logs/graph_group_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/graph_join_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/graph_join_rule.jtl -j jmeter_logs/graph_join_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/graph_mix_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/graph_mix_rule.jtl -j jmeter_logs/graph_mix_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/graph_window_rule.jmx -Dfvt="$fvt_dir" -l jmeter_logs/graph_window_rule.jtl -j jmeter_logs/graph_window_rule.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/lookup_table_memory.jmx -Dfvt="$fvt_dir" -l jmeter_logs/lookup_table_memory.jtl -j jmeter_logs/lookup_table_memory.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/lookup_table_redis.jmx -Dfvt="$fvt_dir" -l jmeter_logs/lookup_table_redis.jtl -j jmeter_logs/lookup_table_redis.log
echo -e "---------------------------------------------\n"

/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/lookup_table_sql.jmx -Dfvt="$fvt_dir" -l jmeter_logs/lookup_table_sql.jtl -j jmeter_logs/lookup_table_sql.log
echo -e "---------------------------------------------\n"

echo -e "-------------------- ecp acceptance test ------------------------\n"
/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/ecp_acceptance/data_import_export.jmx -Dfvt="$fvt_dir" -l jmeter_logs/lookup_table_sql.jtl -j jmeter_logs/lookup_table_sql.log
echo -e "---------------------------------------------\n"
