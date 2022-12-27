# Data Import/Export Management

The eKuiper rule command line tools allows to import and export all the Data.

## Data Format

The file format for importing and exporting Data is JSON, which can contain : `streams`, `tables`, `rules`, `plugin`, `source yaml` and so on. Each type holds the the key-value pair of the name and the creation statement. In the following example file, we define stream 、rules、table、plugin、source config、sink config

```json
{
  "streams": {
    "demo": "CREATE STREAM demo () WITH (DATASOURCE=\"users\", FORMAT=\"JSON\")"
  },
  "tables": {
    "T110":"\n CREATE TABLE T110\n (\n S1 string\n )\n WITH (DATASOURCE=\"test.json\", FORMAT=\"json\", TYPE=\"file\", KIND=\"scan\", );\n "
  },
  "rules": {
    "rule1": "{\"id\": \"rule1\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{\"log\": {}}]}",
    "rule2": "{\"id\": \"rule2\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{  \"log\": {}}]}"
  },
  "nativePlugins":{
    "sinks_file":"fail to download file file:///root/ekuiper-jran/_plugins/ubuntu/sinks/file_amd64.zip: stat /root/ekuiper-jran/_plugins/ubuntu/sinks/file_amd64.zip: no such file or directory",
    "sinks_tdengine":"fail to download file file:///root/ekuiper-jran/_plugins/ubuntu/sinks/tdengine_amd64.zip: stat /root/ekuiper-jran/_plugins/ubuntu/sinks/tdengine_amd64.zip: no such file or directory",
    "sources_random":"fail to download file file:///root/ekuiper-jran/_plugins/ubuntu/sources/random_amd64.zip: stat /root/ekuiper-jran/_plugins/ubuntu/sources/random_amd64.zip: no such file or directory"
  },
  "portablePlugins":{
  },
  "sourceConfig":{
    "mqtt":"{\"td\":{\"insecureSkipVerify\":false,\"password\":\"public\",\"protocolVersion\":\"3.1.1\",\"qos\":1,\"server\":\"tcp://broker.emqx.io:1883\",\"username\":\"admin\"},\"test\":{\"insecureSkipVerify\":false,\"password\":\"public\",\"protocolVersion\":\"3.1.1\",\"qos\":1,\"server\":\"tcp://127.0.0.1:1883\",\"username\":\"admin\"}}"
  },
  "sinkConfig":{
    "edgex":"{\"test\":{\"bufferLength\":1024,\"contentType\":\"application/json\",\"enableCache\":false,\"format\":\"json\",\"messageType\":\"event\",\"omitIfEmpty\":false,\"port\":6379,\"protocol\":\"redis\",\"runAsync\":false,\"sendSingle\":true,\"server\":\"localhost\",\"topic\":\"application\",\"type\":\"redis\"}}"
  },
  "connectionConfig":{
  },
  "Service":{
  },
  "Schema":{
  }
}
```

## Import Data

The API resets all existing data and then imports the new data into the system. 

```shell
# bin/kuiper import data -f myrules.json -s false
```

## Import Data Status

This API returns Data import errors. If all returns are empty, it means that the import is completely successful.

```shell
# bin/kuiper getstatus import
```

## Data Export

This command exports the Data to the specified file. 

```shell
# bin/kuiper export data myrules.json
```