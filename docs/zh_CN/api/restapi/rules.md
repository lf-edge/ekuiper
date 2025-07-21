# 规则管理

eKuiper REST api 可以管理规则，例如创建、显示、删除、描述、启动、停止和重新启动规则。

## 创建规则

该 API 接受 JSON 内容并创建和启动规则。

```shell
POST http://localhost:9081/rules
```

请求示例：

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM demo",
  "actions": [{
    "log":  {}
  }]
}
```

## 展示规则

该 API 用于显示服务器中定义的所有规则和简要状态描述。

```shell
GET http://localhost:9081/rules
```

响应示例：

```json
[
  {
    "id": "rule1",
    "status": "Running"
  },
  {
     "id": "rule2",
     "status": "Stopped: canceled by error."
  }
]
```

## 描述规则

该 API 用于打印规则的详细定义。

```shell
GET http://localhost:9081/rules/{id}
```

路径参数  `id` 是规则的 id 或名称。

响应示例：

```json
{
  "sql": "SELECT * from demo",
  "actions": [
    {
      "log": {}
    },
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```

## 更新规则

该 API 接受 JSON 内容并更新规则。

```shell
PUT http://localhost:9081/rules/{id}
```

路径参数  `id` 是原有规则的 id 或名称。

请求示例：

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM demo",
  "actions": [{
    "log":  {}
  }]
}
```

## 删除规则

该 API 用于删除规则。

```shell
DELETE http://localhost:9081/rules/{id}
```

## 启动规则

该 API 用于开始运行规则。

```shell
POST http://localhost:9081/rules/{id}/start
```

## 停止规则

该 API 用于停止运行规则。

```shell
POST http://localhost:9081/rules/{id}/stop
```

## 重启规则

该 API 用于重启规则。

```shell
POST http://localhost:9081/rules/{id}/restart
```

## 获取规则的状态

该命令用于获取规则的状态。 如果规则正在运行，则将实时检索状态指标。 状态可以是：

- $metrics
- 停止： $reason

```shell
GET http://localhost:9081/rules/{id}/status
```

响应示例：

```shell
{
    "lastStartTimestamp": 0,
    "lastStopTimestamp":0,
    "nextStartTimestamp":0,
    "source_demo_0_records_in_total":5,
    "source_demo_0_records_out_total":5,
    "source_demo_0_exceptions_total":0,
    "source_demo_0_process_latency_ms":0,
    "source_demo_0_buffer_length":0,
    "source_demo_0_last_invocation":"2020-01-02T11:28:33.054821",
    ...
    "op_filter_0_records_in_total":5,
    "op_filter_0_records_out_total":2,
    "op_filter_0_exceptions_total":0,
    "op_filter_0_process_latency_ms":0,
    "op_filter_0_buffer_length":0,
    "op_filter_0_last_invocation":"2020-01-02T11:28:33.054821",
    ...
}
```

其中，以下状态分别代表了规则上次启停的 unix 时间戳，当规则时周期性规则时，可以通过 `nextStartTimestamp` 查看规则下次启动的 unix 时间戳。

```shell
{
    "lastStartTimestamp": 0,
    "lastStopTimestamp":0,
    "nextStartTimestamp":0,
    ...
}
```

## 获取所有规则的状态

该命令用于获取所有规则的状态。 如果规则正在运行，则将实时检索状态指标。

```shell
GET http://localhost:9081/rules/status/all
```

## 验证规则

该 API 用于验证规则。

```shell
POST http://localhost:9081/rules/validate
```

请求示例：

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM demo",
  "actions": [{
    "log":  {}
  }]
}
```

对于 API，以下是状态码的含义说明：
- 如果请求体不正确，将返回状态码 400，表示发送了一个无效的请求。
- 如果规则验证未通过，将返回状态码 422，表示规则无效。
- 如果规则通过验证，将返回状态码 200，表示规则有效且验证通过。

## 查询规则计划

该 API 用于查询 SQL 所转换的计划

```shell
GET  http://localhost:9081/rules/{id}/explain
```

## 获取规则 CPU 信息

```shell
GET http://localhost:9081/rules/usage/cpu

{
    "rule1": 220,
    "rule2": 270
}
```

获取所有规则在过去 30s 内的所使用的 CPU 时间，单位为毫秒

## 重置标签

该 API 用于给规则重置标签

```shell
PUT /rules/{id}/tags

{
  "tags": ["t1","t2"]
}
```

## 添加标签

该 API 用于给规则添加标签

```shell
PATCH /rules/{id}/tags

{
  "tags": ["t1","t2"]
}
```

## 删除标签

该 API 用于给规则删除标签

```shell
DELETE /rules/{id}/tags

{
  "tags": ["t1","t2"]
}
```

## 根据标签查询规则

该 API 用于根据给定标签查询包含该标签的规则们，返回符合条件的规则名列表

```shell
GET /rules/tags/match

{
  "tags": ["t1","t2"]
}
```
