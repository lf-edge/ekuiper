# InfluxDB 目标（Sink）

该插件将分析结果发送到 InfluxDB 中。

## 编译部署插件

在编译之前，请对源代码做如下更改：

- 在 `go.mod` 文件中增加对 InfluxDB 库文件的引用
- 把文件 `plugins/sinks/influxdb.go` 中的第一行 `// +build plugins` 删除

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sinks/InfluxDB.so extensions/sinks/influxdb/influxdb.go
# zip influx.zip plugins/sinks/InfluxDB.so
# cp influx.zip /root/tomcat_path/webapps/ROOT/
# bin/kuiper create plugin sink influx -f /tmp/influxPlugin.txt
# bin/kuiper create rule influx -f /tmp/influxRule.txt
```

重新启动 eKuiper 服务器以激活插件。

## 属性

| 属性名称         | 会否可选 | 说明               |
|--------------|------|------------------|
| addr         | 是    | InfluxDB 的地址      |
| measurement  | 是    | InfluxDb 的测量（如表名） |
| username     | 否    | InfluxDB 登陆用户名    |
| password     | 否    | InfluxDB 登陆密码     |
| databasename | 是    | InfluxDB 数据库名称     |
| tagkey       | 是    | InfluxDB 的标签键     |
| tagvalue     | 是    | InfluxDB 的标签值     |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

## 示例用法

下面是选择温度大于 50 度的样本规则，和一些配置文件仅供参考。

### /tmp/influxRule.txt

```json
{
  "id": "influx",
  "sql": "SELECT * from  demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "influx":{
       "addr": "http://192.168.100.245:8086",
       "username": "",
       "password": "",
       "measurement": "test",
       "databasename": "databasename",
       "tagkey": "tagkey",
       "tagvalue": "tagvalue",
       "fields": ["humidity", "temperature", "pressure"]
      }
    }
  ]
}
```

### /tmp/influxPlugin.txt

```json
{
  "file":"http://localhost:8080/influx.zip"
}
```

### plugins/go.mod

```go
module plugins

go 1.14

require (
        github.com/lf-edge/ekuiper v0.0.0-20200323140757-60d00241372b
        github.com/influxdata/influxdb-client-go v1.2.0
        github.com/influxdata/influxdb1-client v0.0.0-20200515024757-02f0bf5dbca3 // indirect
)

replace github.com/lf-edge/ekuiper => /root/goProject/ekuiper

```
