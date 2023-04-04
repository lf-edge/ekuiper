# InfluxDB Sink

The sink will publish the result into a InfluxDB `V2.X` .

## Compile & deploy plugin

Please make following update before compile the plugin,

- Add Influxdb library reference in `go.mod`.
- Remove the first line `// +build plugins` of file `plugins/sinks/influx.go`.

### build in shell
```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sinks/influx2.so extensions/sinks/influx/influx2.go
# zip influx2.zip plugins/sinks/influx2.so
# cp influx.zip /root/tomcat_path/webapps/ROOT/
# bin/kuiper create plugin sink influx2 -f /tmp/influxPlugin.txt
# bin/kuiper create rule influx2 -f /tmp/influxRule.txt
```

### build with image
```
docker build -t demo/plugins:v1 -f build/plugins/Dockerfile .
docker run demo/plugins:v1
docker cp  90eae15a7245:/workspace/_plugins/debian/sinks /tmp
```
Dockerfile like this：
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
add this in Makefile：
```
PLUGINS_CUSTOM := sinks/influx2

.PHONY: plugins_c $(PLUGINS_CUSTOM)
plugins_c: $(PLUGINS_CUSTOM)

$(PLUGINS_CUSTOM): PLUGIN_TYPE = $(word 1, $(subst /, , $@))
$(PLUGINS_CUSTOM): PLUGIN_NAME = $(word 2, $(subst /, , $@))
$(PLUGINS_CUSTOM):
	@$(CURDIR)/build-plugins.sh $(PLUGIN_TYPE) $(PLUGIN_NAME)
```

Restart the eKuiper server to activate the plugin.

## Properties

| Property name | Optional | Description                                       |
|---------------|----------|---------------------------------------------------|
| addr          | true     | The addr of the InfluxDB                          |
| measurement   | true     | The measurement of the InfluxDb (like table name) |
| org           | false    | The InfluxDB organization                         |
| bucket        | false    | The InfluxDB bucket                               |
| token         | false    | The token of access InfluxDB                      |
| tagKey        | true     | The tag key of the InfluxDB                       |
| tagValue      | true     | The tag value of the InfluxDB                     |
| fields        | true     | The column of the InfluxDB,split with ","         |

Other common sink properties are supported. Please refer to the [sink common properties](../overview.md#common-properties) for more information.

## Sample usage

Below is a sample for selecting temperature great than 50 degree, and some profiles only for your reference.

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
        "fields": "humidity,temperature,pressure"
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

replace github.com/lf-edge/ekuiper => /root/goProject/kuiper

```
