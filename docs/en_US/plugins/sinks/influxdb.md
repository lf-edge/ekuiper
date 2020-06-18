# InfluxDB Sink

The sink will publish the result into a InfluxDB.

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
| addr          | true     | The addr of the InfluxDB |
| measurement   | true     | The measurement of the InfluxDb (like table name) |
| username      | false    | The InfluxDB login username |
| password      | false    | The InfluxDB login password |
| databasename  | true     | The database of the InfluxDB |
| tagkey        | true     | The tag key of the InfluxDB |
| tagvalue      | true     | The tag value of the InfluxDB |
## Sample usage

Below is a sample for selecting temperature great than 50 degree, and the files for creating plugin.

####/tmp/influxRule.txt
```json
{
  "id": "influx",
  "sql": "SELECT * from  demo_stream where temperature < 50",
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
       "tagvalue": "tagvalue"
      }
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

