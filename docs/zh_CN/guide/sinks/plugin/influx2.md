# InfluxDB 目标（Sink）

该插件将分析结果发送到 InfluxDB V2.X 中。
## 编译部署插件

在编译之前，请对源代码做如下更改：

- 在 `go.mod` 文件中增加对 InfluxDB 库文件的引用

### 本地构建
```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sinks/influx2.so extensions/sinks/influx/influx2.go
# zip influx2.zip plugins/sinks/influx2.so
# cp influx.zip /root/tomcat_path/webapps/ROOT/
# bin/kuiper create plugin sink influx2 -f /tmp/influxPlugin.txt
# bin/kuiper create rule influx2 -f /tmp/influxRule.txt
```

### 镜像构建
```
docker build -t demo/plugins:v1 -f build/plugins/Dockerfile .
docker run demo/plugins:v1
docker cp  90eae15a7245:/workspace/_plugins/debian/sinks /tmp
```
Dockerfile 如下所示：
```
## plase check go version that kuiper used
ARG GO_VERSION=1.18.5
FROM ghcr.io/lf-edge/ekuiper/base:$GO_VERSION-debian AS builder
WORKDIR /workspace
ADD . /workspace/
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN make plugins_c
CMD ["sleep","3600"]
```
在Makefile中添加：
```
PLUGINS_CUSTOM := sinks/influx2

.PHONY: plugins_c $(PLUGINS_CUSTOM)
plugins_c: $(PLUGINS_CUSTOM)

$(PLUGINS_CUSTOM): PLUGIN_TYPE = $(word 1, $(subst /, , $@))
$(PLUGINS_CUSTOM): PLUGIN_NAME = $(word 2, $(subst /, , $@))
$(PLUGINS_CUSTOM):
	@$(CURDIR)/build-plugins.sh $(PLUGIN_TYPE) $(PLUGIN_NAME)
```

重新启动 eKuiper 服务器以激活插件。

## 属性

| 属性名称        | 是否可选 | 说明                 |
|-------------|------|--------------------|
| addr        | 是    | InfluxDB 的地址        |
| measurement | 是    | InfluxDB 的测量（如表名）   |
| org         | 否    | InfluxDB 存储组织       |
| bucket      | 否    | InfluxDB 存储 Bucket   |
| token       | 否    | InfluxDB 访问 Token    |
| tagKey      | 是    | InfluxDB 的标签键       |
| tagValue    | 是    | InfluxDB 的标签值       |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

## 示例用法

下面是选择温度大于50度的样本规则，和一些配置文件仅供参考。

### /tmp/influxRule.txt
```json
{
  "id": "influx",
  "sql": "SELECT * from  demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "influx2":{
       "addr": "http://192.168.100.245:8086",
       "token": "test_token",
       "org": "admin",
       "measurement": "test",
       "bucket": "bucketName",
       "tagKey": "tagKey",
       "tagValue": "tagValue",
       "fields": ["humidity", "temperature", "pressure"]
      }
    }
  ]
}
```
### /tmp/influxPlugin.txt
```json
{
  "file":"http://localhost:8080/influx2.zip"
}
```
### plugins/go.mod
```
module plugins

go 1.18

require (
        github.com/lf-edge/ekuiper v0.0.0-20220727015637-7d6f5c447110
        github.com/influxdata/influxdb-client-go/v2 v2.10.0
        github.com/influxdata/line-protocol v0.0.0-20200327222509-2487e7298839 // indirect
)

replace github.com/lf-edge/ekuiper => /root/goProject/ekuiper

```
