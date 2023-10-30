# Websocket 目标 (Sink)

## 属性

| 属性名称         | 是否必填 | 说明                                       |
|--------------|------|------------------------------------------|
| addr         | 是    | websocket server 的地址，如: 127.0.0.1:8080   |
| path     | 是    | websocket server 的 url path，如: /api/data |
| insecureSkipVerify | 是   | 是否忽略 SSL 验证                              |
| certificationPath  | 是   | websocket 客户端 ssl 验证的 crt 文件路径           |
| privateKeyPath     | 是   | websocket 客户端 ssl 验证的 key 文件路径               |
| rootCaPath         | 是   | websocket 客户端 ssl 验证的 ca 证书文件路径              |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

### /tmp/websocket.txt

```json
{
  "id": "redis",
  "sql": "SELECT * from demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "websocket":{
        "addr": "127.0.0.1:8080",
        "password": "/api/data"
      }
    }
  ]
}
```
