# InfluxDB Sink

该插件将分析结果发送到InfluxDB中

## 先决条件
```shell
# vi plugins/sinks/influxdb.go
在influxdb.go文件中修改下面的几个参数为你的值
[addrIp,databasename,measurement,tagkey,tagvalue,filed1,filed2..]
```

## 编译插件&创建插件

```shell
# cd $kuiper_src
# go build --buildmode=plugin -o plugins/sinks/InfluxDB.so plugins/sinks/influxdb.go
# zip influx.zip plugins/sinks/InfluxDB.so
# cp influx.zip /root/tomcat_path/webapps/ROOT/
# bin/cli create plugin sink influx -f /tmp/influxPlugin.txt
# bin/cli create rule influx -f /tmp/influxRule.txt
```

重新启动Kuiper服务器以激活插件。

## 属性

| Property name | Optional | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |

## 示例用法

下面是选择温度大于50度的样本规则，和创建插件时候的配置文件。

####/tmp/influxRule.txt
```json
{
  "id": "influx",
  "sql": "SELECT * from  demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "influx":{}
    }
  ]
}
```
####/tmp/influxPlugin.txt
```json
{
  "file":"http://localhost:8080/influx.zip"
}
```

