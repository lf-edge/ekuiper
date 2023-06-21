# InfluxDB Sink

The sink will publish the result into a InfluxDB.

## Compile & deploy plugin

Please make following update before compile the plugin,

- Add Influxdb library reference in `go.mod`.
- Remove the first line `// +build plugins` of file `plugins/sinks/influx.go`.

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sinks/Influx.so extensions/sinks/influx/influx.go
# zip influx.zip plugins/sinks/Influx.so
# cp influx.zip /root/tomcat_path/webapps/ROOT/
# bin/kuiper create plugin sink influx -f /tmp/influxPlugin.txt
# bin/kuiper create rule influx -f /tmp/influxRule.txt
```

Restart the eKuiper server to activate the plugin.

## Properties

| Property name | Optional | Description                                       |
|---------------|----------|---------------------------------------------------|
| addr          | true     | The addr of the InfluxDB                          |
| measurement   | true     | The measurement of the InfluxDb (like table name) |
| username      | false    | The InfluxDB login username                       |
| password      | false    | The InfluxDB login password                       |
| databasename  | true     | The database of the InfluxDB                      |
| tagkey        | true     | The tag key of the InfluxDB                       |
| tagvalue      | true     | The tag value of the InfluxDB                     |

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

replace github.com/lf-edge/ekuiper => /root/goProject/kuiper

```
