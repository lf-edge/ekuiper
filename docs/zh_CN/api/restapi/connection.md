# 连接管理

## 连通性检查

通过 API 检查 eKuiper 连接的连通性

### sink 端连接检查

```shell
POST http://localhost:9081/metadata/sinks/connection/{sink}
{
  "configuration": "xxxx"
}
```

sink 端连接检查会根据传入的 sinkType 和配置来检查连接的连通性，以 mysql Sink 为例:

```shell
POST http://localhost:9081/metadata/sinks/connection/sql
{
  "url": "mysql://root@127.0.0.1:4000/test",
  "table": "test",
  "fields": ["a","b","c"]
}
```

### source 端连接检查

```shell
POST http://localhost:9081/metadata/sources/connection/{source}
{
  "configuration": "xxxx"
}
```

source 端连接检查会根据传入的 sourceType 和配置来检查连接的连通性，以 mysql Source 为例:

```shell
POST http://localhost:9081/metadata/sources/connection/sql
{
  "url": "mysql://root@127.0.0.1:4000/test",
}
```

## websocket 连接管理

通过 API 管理 eKuiper websocket 的连接

### 创建 websocket endpoint

```shell
POST http://localhost:9081/connection/websocket
```

请求示例：

```json
{
  "endpoint": "/xxxx"
}
```

### 删除 websocket endpoint

```shell
DELETE http://localhost:9081/connection/websocket
```

请求示例：

```json
{
  "endpoint": "/xxxx"
}
```

### 查看 websocket endpoint

```shell
GET http://localhost:9081/connection/websocket
```

请求示例：

```json
{
  "endpoint": "/xxxx"
}
```
