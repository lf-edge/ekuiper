# Redis Sink

The sink will publish the result into redis.

## Compile & deploy plugin
The plugin source code put in the extensions directory, but need build in the ekuiper root path
```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sinks/Redis.so extensions/sinks/redis/redis.go
# zip redis.zip plugins/sinks/Redis.so
# cp redis.zip /root/tomcat_path/webapps/ROOT/
# bin/kuiper create plugin sink redis -f /tmp/redisPlugin.txt
# bin/kuiper create rule redis -f /tmp/redisRule.txt
```

Restart the eKuiper server to activate the plugin.

## Properties

| Property name | Optional | Description                                                                                                                                                                                          |
|---------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| addr          | false    | The addr of the Redis,example: 10.122.48.17:6379                                                                                                                                                     |
| password      | true     | The Redis login password                                                                                                                                                                             |
| db            | false    | The database of the Redis,example: 0                                                                                                                                                                 |
| key           | false    | Select one of the Key, Key and field of Redis data and give priority to field                                                                                                                        |
| field         | true     | This field must exist and be of type string. Otherwise, use the field character as the key. Note: Do not use a data template to configure this value                                                 |
| dataType      | false    | The default Redis data type is string. Note that the original key must be deleted after the Redis data type is changed. Otherwise, the modification is invalid. now only support "list" and "string" |
| expiration    | false    | Timeout duration of Redis data. This parameter is valid only for string data in seconds. The default value is -1                                                                                     |
| rowkindField  | true     | Specify which field represents the action like insert or update. If not specified, all rows are default to insert.                                                                                   |
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
       "addr": "tcp://10.122.48.17:6379",
       "password": "123456",
       "db": "1",
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
