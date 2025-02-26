# TDengine3 目标 （Sink）

## 编译插件

在 eKuiper 项目主目录运行如下命令：

```shell
go build -trimpath --buildmode=plugin -o plugins/sinks/Tdengine3.so extensions/sinks/tdengine3/*.go
```

## 规则 Actions 说明

由于 TDengine 数据库要求表中必须有时间戳字段，所以用户必须告知数据表的时间戳字段名称（必填tsFieldName）。用户可以选择是否提供时间戳数据，若不提供（provideTs=false），时间戳字段的内容由 TDengine 数据库自动生成。

| 名称          | 类型       | 是否必填 | 说明                                                  |
|-------------|----------|------|-----------------------------------------------------|
| host        | string   | 否    | 数据库域名，可以为IP 地址或者域名。其默认值为 localhost。                 |
| port        | int      | 是    | 数据库端口,默认为 6041 。                                    |
| user        | string   | 否    | 用户名，默认值为 `root` 。                                   |
| password    | string   | 否    | 密码，默认值为 `taosdata` 。                                |
| database    | string   | 是    | 数据库名                                                |
| table       | string   | 是    | 表名，可设置[动态属性](../overview.md#动态属性)。                  |
| fields      | []string | 否    | 将要插入的表字段集合。sink 收到的数据和数据库表中均有该字段。若未设置，则所有结果字段写入数据库。 |
| provideTs   | Bool     | 否    | 用户是否提供时间戳字段，默认为否。                                   |
| tsFieldName | String   | 是    | 时间戳字段名称                                             |
| sTable      | String   | 否    | 使用的超级表名称，可设置[动态属性](../overview.md#动态属性)。            |
| tagFields   | []String | 否    | 结果中作为标签的字段。若设置 sTable 属性，则该属性必填。                    |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

## 操作示例

### 创建数据库、超级表

参考以下文档:

```http
https://docs.taosdata.com/get-started/
```

### 创建流

```bash
curl --location --request POST 'http://127.0.0.1:9081/streams' --header 'Content-Type:application/json' --data '{"sql":"create stream demoStream(time string, age BIGINT) WITH ( DATASOURCE = \"device/+/message\", FORMAT = \"json\");"}'
```

### 创建规则

```bash
curl --location --request POST 'http://127.0.0.1:9081/rules' --header 'Content-Type:application/json' --data '{"id":"demoRule","sql":"SELECT * FROM demoStream;","actions":[{"tdengine3":{"provideTs":true,"tsFieldName":"time","user":"root","password":"taosdata","database":"dbName","table":"tableName","fields":["time","age"]}}]}'
```

写入固定表格的例子：

```json
{
  "tdengine3": {
    "host":        "127.0.0.1",
    "port":        6041,
    "user":        "root",
    "password":    "taosdata",
    "database":    "db",
    "table":       "tableName",
    "tsfieldname": "ts"
  }
}
```

写入动态表的例子：

```json
{
  "tdengine3": {
    "sendSingle":   true,
    "host":        "hostname",
    "port":        6041,
    "user":        "root",
    "password":    "taosdata",
    "database":    "db",
    "table":       "{{.tName}}",
    "tsfieldname": "ts"
  }
}
```
