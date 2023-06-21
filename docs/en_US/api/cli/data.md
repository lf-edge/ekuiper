# Data Import/Export Management

The eKuiper rule command line tools allows to import and export the Data.

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
    "functions_image":"{\"name\":\"image\",\"file\":\"https://packages.emqx.net/kuiper-plugins/1.8.1/debian/functions/image_amd64.zip\",\"shellParas\":[]}",
    "sources_video":"{\"name\":\"video\",\"file\":\"https://packages.emqx.net/kuiper-plugins/1.8.1/debian/sources/video_amd64.zip\",\"shellParas\":[]}",
  },
  "portablePlugins":{
  },
  "sourceConfig":{
    "mqtt":"{\"td\":{\"insecureSkipVerify\":false,\"password\":\"public\",\"protocolVersion\":\"3.1.1\",\"qos\":1,\"server\":\"tcp://broker.emqx.io:1883\",\"username\":\"admin\"},\"test\":{\"insecureSkipVerify\":false,\"password\":\"public\",\"protocolVersion\":\"3.1.1\",\"qos\":1,\"server\":\"tcp://127.0.0.1:1883\",\"username\":\"admin\"}}"
  },
  "sinkConfig":{
    "edgex":"{\"test\":{\"bufferLength\":1024,\"contentType\":\"application/json\",\"enableCache\":false,\"format\":\"json\",\"messageType\":\"event\",\"omitIfEmpty\":false,\"port\":6379,\"protocol\":\"redis\",\"sendSingle\":true,\"server\":\"localhost\",\"topic\":\"application\",\"type\":\"redis\"}}"
  },
  "connectionConfig":{
  },
  "Service":{
  },
  "Schema":{
  }
}
```

## Reset And Import Data

The API resets all existing data and then imports the new data into the system.

```shell
# bin/kuiper import data -f myrules.json -s false
```

## Import Data

The API imports the data into the system(overwrite the tables/streams/rules/source config/sink config. install plugins/schema if not exist, else ignore them).

```shell
# bin/kuiper import data -f myrules.json -p true
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

This command exports the specific rules related Data to the specified file.

```shell
# bin/kuiper export data myrules.json -r '["rules1", "rules2"]'
```
