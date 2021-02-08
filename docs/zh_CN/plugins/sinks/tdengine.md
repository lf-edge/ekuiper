## 编译插件

### plugins/go.mod

```go
module plugins

go 1.13

replace github.com/emqx/kuiper => /$kuiper

require (
    github.com/emqx/kuiper v0.0.0-00010101000000-000000000000 // indirect
    github.com/taosdata/driver-go v0.0.0-20200723061832-5be6460b0c20
)
```

```shell
go mod edit -replace github.com/emqx/kuiper=/$kuiper
go build -trimpath --buildmode=plugin -o /$kuiper/plugins/sinks/Tdengine@v1.0.0.so /$kuiper/plugins/sinks/tdengine/tdengine.go
```
### 安装插件
由于 tdengine 插件的运行依赖于 tdengine 客户端，为了便于用户使用，安装插件时将下载 tdengine 客户端。但是 tdengine 客户端版本与其服务器版本一一对应，互不兼容，所以用户必须告知所用 tdengine 服务器版本。
## 规则 Actions 说明

由于 tdengine 数据库要求表中必须有时间戳字段，所以用户必须告知数据表的时间戳字段名称（必填tsFieldName）。用户可以选择是否提供时间戳数据，若不提供（provideTs=false），时间戳字段的内容由 tdengine 数据库自动生成。

| 名称        | 类型     | 是否必填                      | 释义                   |
| ----------- | -------- | ----------------------------- | ---------------------- |
| ip          | string   | 必填                          | 数据库ip               |
| port        | int      | 必填                          | 数据库端口             |
| user        | string   | 必填                          | 用户名                 |
| password    | string   | 必填                          | 密码                   |
| database    | string   | 必填                          | 数据库名               |
| table       | string   | 必填                          | 表名                   |
| fields      | []string | 选填（不填时用数据的key替代） | 表字段集合             |
| provideTs   | Bool     | 必填                          | 用户是否提供时间戳字段 |
| tsFieldName | String   | 必填                          | 时间戳字段名称         |

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

### 发送数据

```bash
mosquitto_pub -h broker.emqx.io -m '{"time":"2020-01-11 18:18:18", "age" : 18}' -t device/device_001/message
```

