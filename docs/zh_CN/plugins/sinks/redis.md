# Redis 目标（Sink）

该插件将分析结果发送到 Redis 中。
## 编译插件&创建插件

redis 源代码在 extensions 目录中，但是需要在 eKuiper 根目录编译
```shell
# cd $eKuiper_src
# go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/sinks/Redis.so extensions/sinks/redis/redis.go
# zip redis.zip plugins/sinks/Redis.so
# cp redis.zip /root/tomcat_path/webapps/ROOT/
# bin/kuiper create plugin sink redis -f /tmp/redisPlugin.txt
# bin/kuiper create rule redis -f /tmp/redisRule.txt
```

重新启动 eKuiper 服务器以激活插件。

## 属性

| 属性名称     | 是否必填 | 说明                     |
| ------------ | -------- | ------------------------ |
| addr          | 是     | Redis 的地址, 例如: 10.122.48.17:6379 |
| password      | 否     | Redis 登陆密码 |
| db            | 是     | Redis 的数据库,例如0 |
| key           | 是     | Redis 数据的 Key， key 与 field 选择其中一个, 优先 field |
| field         | 否     | json 数据某一个属性，配置它作为 redis 数据的 key 值, 例如 deviceName, 该字段必须存在且为 string 类型，否则以 field 字符作为 key。比如 field 属性为 "deviceName", 收到 {“deviceName":"abc"}, 那么存入redis用的key是 "abc"; 收到 {“deviceName": 2}, 那么存入redis用的key是 "deviceName"。 注意:配置该值不要使用数据模板 |
| dataType      | 是     | Redis 数据的类型, 默认是 string, 注意修改类型之后，需在redis中删除原有 key，否则修改无效。目前只支持 "list" 和 "string" |
| expiration    | 是     | 超时时间
## 示例用法

下面是选择温度大于50度的样本规则，和一些配置文件仅供参考。

### ####/tmp/redis.txt
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
### ####/tmp/redisPlugin.txt
```json
{
  "file":"http://localhost:8080/redis.zip"
}
```