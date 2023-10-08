# RedisPub 目标（Sink）

该操作用于将输出消息发布到redis消息通道。

## 属性

| 属性名称         | 是否必填 | 说明                                                    |
|--------------|------|-------------------------------------------------------|
| address      | 是    | Redis 的地址, 例如: 127.0.0.1:6379                         |
| username     | 否    | Redis 登录用户名（如果需要身份验证则填写）                              |
| password     | 否    | Redis 登录密码（如果需要身份验证则填写）                               |
| db           | 是    | Redis 的数据库,例如0                                        |
| channel      | 是    | 用于指定要订阅的 Redis 频道列表。                                  |
| compression  | 否    | 使用指定的压缩方法压缩 Payload。当前支持 zlib, gzip, flate, zstd  算法。 |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

## 示例用法

下面是一个发布压缩数据到本地Redis服务器的示例：

```json
{
  "redis":{
    "address": "127.0.0.1:6379",
    "username": "default",
    "password": "123456",
    "db": 0,
    "channel": "exampleChannel",
    "compression": "zlib"
  }
}
```

这个示例配置用于将数据发布到Redis的"exampleChannel"频道，并应用了zlib压缩。
