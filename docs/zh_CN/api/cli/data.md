# 数据导入导出管理

eKuiper 命令行工具允许您导入导出当前的所有数据。

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

## 导入数据

该 API 接受数据并将其导入系统中。若已有历史遗留数据，则首先清除旧有数据，然后导入。

```shell
# bin/kuiper import data -f myrules.json -s false
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