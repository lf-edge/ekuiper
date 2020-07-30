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
go build --buildmode=plugin -o /$kuiper/plugins/sinks/Taos@v1.0.0.so /$kuiper/plugins/sinks/taos.go
```

## 规则 Actions 说明

| 名称     | 类型     | 是否必填                      | 释义       |
| -------- | -------- | ----------------------------- | ---------- |
| ip       | string   | 必填                          | 数据库ip   |
| port     | int      | 必填                          | 数据库端口 |
| user     | string   | 必填                          | 用户名     |
| password | string   | 必填                          | 密码       |
| database | string   | 必填                          | 数据库名   |
| table    | string   | 必填                          | 表名       |
| fields   | []string | 选填（不填时用数据的key替代） | 表字段集合 |

## 操作示例

### 创建数据库、表，参考以下文档：

```http
https://www.taosdata.com/cn/getting-started/
```

### 创建流

```curl
curl --location --request POST 'http://127.0.0.1:9081/streams' --header 'Content-Type:application/json' --data '{"sql":"create stream demoStream(time string, age BIGINT) WITH ( DATASOURCE = \"device/+/message\", FORMAT = \"json\");"}'
```

### 创建规则

```curl
curl --location --request POST 'http://127.0.0.1:9081/rules' --header 'Content-Type:application/json' --data '{"id":"demoRule","sql":"SELECT * FROM demoStream;","actions":[{"taos":{"port":0,"ip":"127.0.0.1","user":"root","password":"taosdata","database":"dbName","table":"tableName","fields":["time","age"]}}]}'
```

### 发送数据

```curl
mosquitto_pub -h broker.emqx.io -m '{"time":"2020-01-11 18:18:18", "age" : 18}' -t device/device_001/message
```

