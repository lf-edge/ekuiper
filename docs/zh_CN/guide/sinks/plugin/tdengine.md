## 编译插件

在 eKuiper 项目主目录运行如下命令：

```shell
go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/sinks/Tdengine@v1.0.0.so extensions/sinks/tdengine/tdengine.go
```
### 安装插件

由于 tdengine 插件的运行依赖于 tdengine 客户端，为了便于用户使用，安装插件时将下载 tdengine 客户端。但是 tdengine 客户端版本与其服务器版本一一对应，互不兼容，所以用户必须告知所用 tdengine 服务器版本。

## 规则 Actions 说明

由于 tdengine 数据库要求表中必须有时间戳字段，所以用户必须告知数据表的时间戳字段名称（必填tsFieldName）。用户可以选择是否提供时间戳数据，若不提供（provideTs=false），时间戳字段的内容由 tdengine 数据库自动生成。

| 名称             | 类型       | 是否必填 | 释义                                                                                                    |
|----------------|----------|--|-------------------------------------------------------------------------------------------------------|
| host           | string   | 否 | 数据库域名，其值必须为域名，即 [FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)，不能为 IP 地址。其默认值为 localhost。 |
| port           | int      | 是 | 数据库端口                                                                                                 |
| user           | string   | 否 | 用户名，默认值为 `root` 。                                                                                     |
| password       | string   | 否 | 密码，默认值为 `taosdata` 。                                                                                  |
| database       | string   | 是 | 数据库名                                                                                                  |
| table          | string   | 是 | 表名，可设置[动态属性](../overview.md#动态属性)。                                                                 |
| fields         | []string | 否 | 将要插入的表字段集合。sink 收到的数据和数据库表中均有该字段。若为设置，则所有结果字段写入数据库。                                                   |
| provideTs      | Bool     | 否 | 用户是否提供时间戳字段，默认为否。                                                                                     |
| tsFieldName    | String   | 是 | 时间戳字段名称                                                                                               |
| sTable         | String   | 否 | 使用的超级表，可设置[动态属性](../overview.md#动态属性)。                                                             |
| tagFields      | []String | 否 | 结果中作为标签的字段。若设置 sTable 属性，则该属性必填。                                                                      |
| tableDataField | String   | 否 | 将 tableDataField 的嵌套值写入数据库。                                          |

## 操作示例

### 创建数据库、表，参考以下文档：

```http
https://www.taosdata.com/cn/getting-started/
```

### 创建流

```bash
curl --location --request POST 'http://127.0.0.1:9081/streams' --header 'Content-Type:application/json' --data '{"sql":"create stream demoStream(time string, age BIGINT) WITH ( DATASOURCE = \"device/+/message\", FORMAT = \"json\");"}'
```

### 创建规则

```bash
curl --location --request POST 'http://127.0.0.1:9081/rules' --header 'Content-Type:application/json' --data '{"id":"demoRule","sql":"SELECT * FROM demoStream;","actions":[{"tdengine":{"provideTs":true,"tsFieldName":"time","port":0,"ip":"127.0.0.1","user":"root","password":"taosdata","database":"dbName","table":"tableName","fields":["time","age"]}}]}'
```

写入固定表格的例子：

```json
{
  "tdengine": {
    "host":        "hostname",
    "port":        6030,
    "user":        "root",
    "password":    "taosdata",
    "database":    "db",
    "table":       "tableName",
    "tsfieldname": "ts"
  }
}
```

写入动态表的例子：

```json lines
{
  "tdengine": {
    "host":        "hostname",
    "port":        6030,
    "database":    "dab",
    "table":       "{{.table}}", // 动态值，从结果中的 table 字段获取
    "tsfieldname": "ts",
    "fields":      []string{"f1", "f2"}, // 结果中的 f1, f2 字段写入数据库中的 f1, f2 列
    "sTable":      "myStable", // 超级表名，也可以动态
    "tagFields":   []string{"f3","f4"} // 结果中的 f3, f4 字段的值按顺序作为标签值写入
  }
}
```



根据 tableDataField 配置将结果写入数据库:

以下配置将 telemetry 字段的对应值写入数据库

```json
{
  "telemetry": [{
    "temperature": 32.32,
    "humidity": 80.8,
    "f3": "f3tagValue",
    "f4": "f4tagValue",
    "ts": 1388082430
  },{
    "temperature": 34.32,
    "humidity": 81.8,
    "f3": "f3tagValue",
    "f4": "f4tagValue",
    "ts": 1388082440
  }]
}
```

```json lines
{
  "tdengine": {
    "host":        "hostname",
    "port":        6030,
    "database":    "dab",
    "table":       "tableName", // dynamic value, get from the table field of the result
    "tsfieldname": "ts",
    "fields":      []string{"temperature", "humidity"}, // Write f1, f2 fields in result into f1, f2 columns in the db
    "sTable":      "myStable", // super table name, also allow dynamic
    "tableDataField":      "telemetry", // write values of telemetry field into database
    "tagFields":   []string{"f3","f4"} // Write f3, f4 fields' values in the result as tags in order
  }
}
```


