# 连接管理

## 连接管理

规则中的 Source/Sink 可以以连接的形式被独立创建、管理。

### 创建连接

创建连接去要提供连接的 id, 类型和配置参数。目前已经支持了 `mqtt`/`nng`/`httppush`/`websocket`/`edgex`/`sql` 类型的连接，这里以创建 mqtt 连接为例。

```shell
POST http://localhost:9081/connections
{
  "id": "connecton-1",
  "typ":"mqtt",
  "props": {
    "server": "tcp://127.0.0.1:1883"
  }
}
```

### 更新连接

更新连接要提供连接的 id, 类型和配置参数。目前已经支持了 `mqtt`/`nng`/`httppush`/`websocket`/`edgex`/`sql` 类型的连接，这里以更新 mqtt 连接为例。如果连接被规则引用中，则无法被更新。

```shell
PUT http://localhost:9081/connections/connection-1
{
  "id": "connecton-1",
  "typ":"mqtt",
  "props": {
    "server": "tcp://127.0.0.1:1883"
  }
}
```

### 获取所有连接信息

```shell
GET http://localhost:9081/connections
```

返回所有连接的信息和连接状态。

### 获取单个连接状态

```shell
GET http://localhost:9081/connections/{id}
```

### 删除单个连接

删除连接时会是否有规则正在使用连接，如果存在规则正在使用连接，那么连接将无法被删除。

```shell
DELETE http://localhost:9081/connections/{id}
```

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
