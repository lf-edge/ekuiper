# Data Import/Export Management

eKuiper REST api allows to import or export all data.

## Data Format

The file format for importing and exporting data is JSON, which can contain : `streams`, `tables`, `rules`, `plugin`, `source yaml` and so on. Each type holds the the key-value pair of the name and the creation statement. In the following example file, we define stream 、rules、table、plugin、source config、sink config

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

The API resets all existing data and then imports the new data into the system. The API supports specifying data by means of text content or file URIs.


Example 1: Import by text content


```shell
POST http://{{host}}/data/import
Content-Type: application/json

{
  "content": "$规则集 json 内容"
}
```

Example 2: Import by file URI

```shell
POST http://{{host}}/data/import
Content-Type: application/json

{
  "file": "file:///tmp/a.json"
}
```

Example 3: Import data via file URI and exit (for plug-ins and static schema updates, users need to ensure that eKuiper can be restarted after exiting)

```shell
POST http://{{host}}/data/import?stop=1
Content-Type: application/json

{
  "file": "file:///tmp/a.json"
}
```

## Import data status


This API returns data import errors. If all returns are empty, it means that the import is completely successful.

```shell
GET http://{{host}}/data/import/status
```

Example 1: The data import is completely successful

```shell
GET http://{{host}}/data/import/status
Content-Type: application/json

{
  "streams":{},
  "tables":{},
  "rules":{},
  "nativePlugins":{},
  "portablePlugins":{},
  "sourceConfig":{},
  "sinkConfig":{},
  "connectionConfig":{},
  "Service":{},
  "Schema":{}}
```

Example 2: Failed to import plugin

```shell
GET http://{{host}}/data/import/status
Content-Type: application/json

{
  "streams":{},
  "tables":{},
  "rules":{},
  "nativePlugins":{
    "sinks_file":"fail to download file file:///root/ekuiper-jran/_plugins/ubuntu/sinks/file_amd64.zip: stat /root/ekuiper-jran/_plugins/ubuntu/sinks/file_amd64.zip: no such file or directory",
    "sinks_tdengine":"fail to download file file:///root/ekuiper-jran/_plugins/ubuntu/sinks/tdengine_amd64.zip: stat /root/ekuiper-jran/_plugins/ubuntu/sinks/tdengine_amd64.zip: no such file or directory",
    "sources_random":"fail to download file file:///root/ekuiper-jran/_plugins/ubuntu/sources/random_amd64.zip: stat /root/ekuiper-jran/_plugins/ubuntu/sources/random_amd64.zip: no such file or directory"},
  "portablePlugins":{},
  "sourceConfig":{},
  "sinkConfig":{},
  "connectionConfig":{},
  "Service":{},
  "Schema":{}}
```

## Data Export

The export API returns a file to download.

```shell
GET http://{{host}}/data/export
```