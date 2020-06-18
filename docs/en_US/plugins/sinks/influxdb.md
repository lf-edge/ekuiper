# InfluxDB Sink

The sink will publish the result into a InfluxDB.

## Precondition
```shell
# vi plugins/sinks/influxdb.go
Modify the following parameters by yourself
[addrIp,databasename,measurement,tagkey,tagvalue,filed1,filed2..]
```

## Compile & deploy plugin

```shell
# cd $kuiper_src
# go build --buildmode=plugin -o plugins/sinks/InfluxDB.so plugins/sinks/influxdb.go
# zip influx.zip plugins/sinks/InfluxDB.so
# cp influx.zip /root/tomcat_path/webapps/ROOT/
# bin/cli create plugin sink influx -f /tmp/influxPlugin.txt
# bin/cli create rule influx -f /tmp/influxRule.txt
```

Restart the Kuiper server to activate the plugin.

## Properties

| Property name | Optional | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |

## Sample usage

Below is a sample for selecting temperature great than 50 degree, and the files for creating plugin.

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

