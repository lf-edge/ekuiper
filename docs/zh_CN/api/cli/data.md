# 数据导入导出管理

eKuiper 命令行工具允许您导入导出当前数据。

## 数据格式

导入导出数据的文件格式为 JSON, 包含流 `stream`，表 `table`， 规则 `rule`，插件 `plugin`，源配置 `source yaml` 等。每种类型保存名字和创建语句的键值对。在以下示例文件中，我们定义了流、规则、表、插件、源配置、目标动作配置。

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

## 清除旧数据并导入新数据

该 API 接受数据并将其导入系统中。若已有历史遗留数据，则首先清除旧有数据，然后导入。

```shell
# bin/kuiper import data -f myrules.json -s false
```

## 导入新数据

该 API 接受数据并将其导入系统中(覆盖 tables/streams/rules/source config/sink 相关数据. 如果 plugins/schema 在系统中不存在， 那么安装，否则忽略相关配置)。

```shell
# bin/kuiper import data -f myrules.json -p true
```

## 导入数据状态查询

该 API 返回数据导入出错情况，如所有返回为空，则代表导入完全成功。

```shell
# bin/kuiper getstatus import
```

## 导出数据

导出 API 返回二进制流，在浏览器使用时，可选择下载保存的文件路径。

```shell
# bin/kuiper export data myrules.json
```

导出特定规则相关数据

```shell
# bin/kuiper export data myrules.json -r '["rules1", "rules2"]'
```