# Websocket 目标 (Sink)

## 属性

| 属性名称         | 是否必填 | 说明                                                                                                                                                                        |
|--------------|------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| addr         | 是    | websocket server 的地址，如: 127.0.0.1:8080                                                                                                  |
| path     | 是    | websocket server 的 url path，如: /api/data                                                                                                    |

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
