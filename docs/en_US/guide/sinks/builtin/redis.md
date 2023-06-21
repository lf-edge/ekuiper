# Redis Sink

The sink will publish the result into redis.

## Properties

| Property name | Optional | Description                                                                                                                                                                                                                                                                                           |
|---------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| addr          | false    | The addr of the Redis,example: 10.122.48.17:6379                                                                                                                                                                                                                                                      |
| password      | true     | The Redis login password                                                                                                                                                                                                                                                                              |
| db            | false    | The database of the Redis,example: 0                                                                                                                                                                                                                                                                  |
| key           | false    | Select one of the Key, Key and field of Redis data and give priority to field, it is only applicable when keyType is ``single``.                                                                                                                                                                      |
| field         | true     | This field must exist. For example, if the field attribute is "deviceName" and {"deviceName":"abc"} is received, then the key used to store in redis is "abc". it is only applicable when keyType is ``single``. Note: Do not use a data template to configure this value                             |
| keyType       | true     | The property that determine the format of data to be stored in redis, can be ``single`` or ``multiple``, and default is ``single``. ``single`` means all data will be save into redis after json marshal as a single value. ``multiple`` means all key-value pair will be saved into redis separately |
| dataType      | false    | The default Redis data type is string. Note that the original key must be deleted after the Redis data type is changed. Otherwise, the modification is invalid. now only support "list" and "string"                                                                                                  |
| expiration    | false    | Timeout duration of Redis data. This parameter is valid only for string data in seconds. The default value is -1                                                                                                                                                                                      |
| rowkindField  | true     | Specify which field represents the action like insert or update. If not specified, all rows are default to insert.                                                                                                                                                                                    |

## Sample usage

Below is a sample for selecting temperature greater than 50 degree, and some profiles only for your reference.

### /tmp/redisRule.txt

```json
{
  "id": "redis",
  "sql": "SELECT * from  demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "redis":{
       "addr": "10.122.48.17:6379",
       "password": "123456",
       "db": 1,
       "dataType": "string",
       "expire": "10000",
       "field": "temperature"
      }
    }
  ]
}
```

### Updatable sample

By specifying the `rowkindField` property, the sink can update according the action specified in that field.

```json
{
  "id": "ruleUpdateAlert",
  "sql":"SELECT * FROM alertStream",
  "actions":[
    {
      "redis": {
        "addr": "127.0.0.1:6379",
        "dataType": "string",
        "field": "id",
        "rowkindField": "action",
        "sendSingle": true
      }
    }
  ]
}
```

### Upsert multiple keys sample

By specifying the ``keyType`` property to be ``multiple``, the sink can update multiple keys' corresponding value in redis

```json
{
  "id": "ruleUpdateAlert",
  "sql":"SELECT * FROM alertStream",
  "actions":[
    {
      "redis": {
        "addr": "127.0.0.1:6379",
        "dataType": "string",
        "keyType": "multiple",
        "sendSingle": true
      }
    }
  ]
}
```

When result map is the following format, the ``temperature`` and ``humidity`` will be saved into redis separately

```json
{
    "temperature": 40.9,
    "humidity": 30.9
}
```
