# Redis 目标（Sink）

## 属性

| 属性名称         | 是否必填 | 说明                                                                                                                                                                        |
|--------------|------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| addr         | 是    | Redis 的地址, 例如: 10.122.48.17:6379                                                                                                                                          |
| password     | 否    | Redis 登陆密码                                                                                                                                                                |
| db           | 是    | Redis 的数据库,例如0                                                                                                                                                            |
| key          | 是    | Redis 数据的 Key， key 与 field 选择其中一个, 优先 field。只有当 keyType 值为 ``single`` 时此配置才有效。                                                                                            |
| field        | 否    | json 数据某一个属性，配置它作为 redis 数据的 key 值, 该字段必须存在。比如 field 属性为 "deviceName", 收到 {“deviceName":"abc"}, 那么存入redis用的key是 "abc"。只有当 keyType 值为 ``single`` 时此配置才有效。注意:配置该值不要使用数据模板 。 |
| keyType      | 否    | 此配置控制 json 数据以整体形式存入或者以键值为单位存入 redis，可选值为 ``single`` 或者 ``multiple``, 默认值为 ``single`` 。当选择 ``single`` 时，将整体数据以 json 形式存入。当选择 ``multiple`` 时， 将多个键值对分别存储进 redis。           |
| dataType     | 是    | Redis 数据的类型, 默认是 string, 注意修改类型之后，需在redis中删除原有 key，否则修改无效。目前只支持 "list" 和 "string"                                                                                         |
| expiration   | 是    | 超时时间                                                                                                                                                                      |
| rowkindField | 是    | 指定哪个字段表示操作，例如插入或更新。如果不指定，默认所有的数据都是插入操作                                                                                                                                    |
其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

## 示例用法

下面是选择温度大于50度的样本规则，和一些配置文件仅供参考。

### /tmp/redis.txt
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
### /tmp/redisPlugin.txt
```json
{
  "file":"http://localhost:8080/redis.zip"
}
```

### 更新示例

通过指定 `rowkindField` 属性，sink 可以根据该字段中指定的动作进行更新。

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

### Upsert 多个键示例

通过指定 ``keyType`` 属性为 ``multiple``， sink可以更新多个 key 在redis中对应的值

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

当结果集为如下格式时，``temperature`` 和 ``humidity`` 会分别保存到redis中

```json
{
    "temperature": 40.9,
    "humidity": 30.9
}
```
