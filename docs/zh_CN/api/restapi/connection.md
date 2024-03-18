# websocket 连接管理

通过 API 管理 eKuiper websocket 的连接

## 创建 websocket endpoint

```shell
POST http://localhost:9081/connection/websocket
```

请求示例：

```json
{
  "endpoint": "/xxxx"
}
```

## 删除 websocket endpoint

```shell
DELETE http://localhost:9081/connection/websocket
```

请求示例：

```json
{
  "endpoint": "/xxxx"
}
```

## 查看 websocket endpoint

```shell
GET http://localhost:9081/connection/websocket
```

请求示例：

```json
{
  "endpoint": "/xxxx"
}
```
