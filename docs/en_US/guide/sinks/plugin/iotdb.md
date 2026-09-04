# IoTDB Sink

The sink writes data into Apache IoTDB using the native Thrift RPC client. It supports both the tree model and the table model.

## Compile the plugins

In eKuiper source code root path, run the below command:

```shell
go build -trimpath --buildmode=plugin -o plugins/sinks/Iotdb.so extensions/sinks/iotdb/*.go
```

## Properties

Connection properties:

| Property name | Optional | Default value  | Description                                                    |
|---------------|----------|----------------|----------------------------------------------------------------|
| addr          | false    | 127.0.0.1:6667 | IoTDB server address in `host:port` format.                    |
| username      | true     | root           | The username for authentication.                               |
| password      | true     | root           | The password for authentication.                               |
| nodeUrls      | true     | []             | Cluster node URLs. When set, it overrides the `addr` property. |
| timeout       | true     | 5000           | Connection timeout in milliseconds.                            |
| poolSize      | true     | 3              | The size of the connection pool.                               |

Model selection:

| Property name | Optional | Default value | Description                               |
|---------------|----------|---------------|-------------------------------------------|
| model         | false    | tree          | The data model to use: `tree` or `table`. |

Tree model properties (used when `model=tree`):

| Property name | Optional | Default value | Description                                                     |
|---------------|----------|---------------|-----------------------------------------------------------------|
| device        | true     | ""            | The device path, e.g. `root.sg1.dev1`. Required for tree model. |
| isAligned     | true     | false         | Whether to use aligned time series.                             |

Table model properties (used when `model=table`):

| Property name    | Optional | Default value | Description                                                                                              |
|------------------|----------|---------------|----------------------------------------------------------------------------------------------------------|
| database         | true     | ""            | The database name (without the `root.` prefix; it is automatically stripped). Required for table model.   |
| table            | true     | ""            | The target table name. Required for table model.                                                         |
| columnCategories | true     | []            | Column categories corresponding to `measurements` one-to-one: `TAG`, `FIELD`, or `ATTRIBUTE`. Required for table model. |

Data mapping properties:

| Property name | Optional | Default value | Description                                                                                                              |
|---------------|----------|---------------|--------------------------------------------------------------------------------------------------------------------------|
| measurements  | false    | []            | List of measurement / column names.                                                                                      |
| dataTypes     | false    | []            | IoTDB data types corresponding to `measurements` one-to-one: `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `TEXT`, `STRING`, `TIMESTAMP`. |
| tsFieldName   | true     | ""            | The field name of the timestamp (in milliseconds). If not set, the current time will be used; when set, every row must contain this field. |
| batchSize     | true     | 10            | The number of rows per tablet write.                                                                                     |

Other common sink properties including batch settings are supported. Please refer to
the [sink common properties](../overview.md#common-properties) for more information.

## Sample usage

### Tree model example

Below is a sample rule for selecting temperature greater than 50 and writing into IoTDB using the tree model.

```json
{
  "id": "iotdb_tree",
  "sql": "SELECT * from demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "iotdb": {
        "addr": "127.0.0.1:6667",
        "username": "root",
        "password": "root",
        "model": "tree",
        "device": "root.sg1.d1",
        "measurements": ["temperature", "humidity"],
        "dataTypes": ["FLOAT", "FLOAT"],
        "tsFieldName": "ts",
        "batchSize": 10
      }
    }
  ]
}
```

### Table model example

Below is a sample rule for selecting temperature greater than 50 and writing into IoTDB using the table model.

```json
{
  "id": "iotdb_table",
  "sql": "SELECT * from demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "iotdb": {
        "addr": "127.0.0.1:6667",
        "username": "root",
        "password": "root",
        "model": "table",
        "database": "iot_data",
        "table": "sensor_data",
        "measurements": ["device_id", "temperature", "humidity"],
        "dataTypes": ["STRING", "FLOAT", "FLOAT"],
        "columnCategories": ["TAG", "FIELD", "FIELD"],
        "tsFieldName": "ts",
        "batchSize": 10
      }
    }
  ]
}
```

## Data Types

The following IoTDB data types are supported in the `dataTypes` property:

| Data type  | Description                     |
|------------|---------------------------------|
| INT32      | 32-bit signed integer           |
| INT64      | 64-bit signed integer           |
| FLOAT      | Single-precision floating point |
| DOUBLE     | Double-precision floating point |
| BOOLEAN    | Boolean value (true / false)    |
| TEXT       | Text string (IoTDB legacy type) |
| STRING     | String value                    |
| TIMESTAMP  | Timestamp value                 |

## Notes

- IoTDB uses Thrift RPC protocol on port 6667 by default.
- For table model, the database is automatically created if it does not exist.
- The `root.` prefix in the `database` field is automatically stripped for table model.
- Missing fields in the data will be written as null values to IoTDB.
