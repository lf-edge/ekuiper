# Sql Sink

The sink will write the result to the database.

## Compile & deploy plugin

This plugin must be used in conjunction with at least a database driver. We are using build tag to determine which driver will be included.
This [repository](https://github.com/lf-edge/ekuiper/tree/master/extensions/sqldatabase/driver) lists all the supported drivers.

This plugin supports `sqlserver\postgres\mysql\sqlite3\oracle` drivers by default. User can compile plugin that only support one driver by himself,
for example, if he only wants mysql, then he can build with build tag `mysql`.

### Default build command
```shell
# cd $eKuiper_src
# go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/sinks/Sql.so extensions/sinks/sql/sql.go
# cp plugins/sinks/Sql.so $eKuiper_install/plugins/sinks
```

### MySql build command
```shell
# cd $eKuiper_src
# go build -trimpath -modfile extensions.mod --buildmode=plugin -tags mysql -o plugins/sinks/Sql.so extensions/sinks/sql/sql.go
# cp plugins/sinks/Sql.so $eKuiper_install/plugins/sinks
```


## Properties

| Property name  | Optional | Description                                                                                                                                                   |
|----------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url            | false    | The url of the target database                                                                                                                                |
| table          | false    | The table name of the result                                                                                                                                  |
| fields         | false    | The fields to be inserted to. The result map and the database should both have these fields. If not specified, all fields in the result map will be inserted. |
| tableDataField | true     | Write the nested values of the tableDataField into database.                                                                                                  |

## Sample usage

Below is a sample for using sql to get the target data and set to mysql database 

```json
{
  "id": "rule",
  "sql": "SELECT stuno as id, stuName as name, format_time(entry_data,\"YYYY-MM-dd HH:mm:ss\") as registerTime FROM SqlServerStream",
  "actions": [
    {
      "log": {
      },
      "sql": {
        "url": "mysql://user:test@140.210.204.147/user?parseTime=true",
        "table": "test",
        "fields": ["id","name","registerTime"]
      }
    }
  ]
}
```


Write values of tableDataField into database:

The following configuration will write telemetry field's values into database

```json
{
  "telemetry": [{
    "temperature": 32.32,
    "humidity": 80.8,
    "ts": 1388082430
  },{
    "temperature": 34.32,
    "humidity": 81.8,
    "ts": 1388082440
  }]
}
```

```json lines
{
  "id": "rule",
  "sql": "SELECT telemetry FROM dataStream",
  "actions": [
    {
      "log": {
      },
      "sql": {
        "url": "mysql://user:test@140.210.204.147/user?parseTime=true",
        "table": "test",
        "fields": ["temperature","humidity"],
        "tableDataField":  "telemetry",
      }
    }
  ]
}
```

