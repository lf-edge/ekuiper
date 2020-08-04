# 规则管理

Kuiper REST api 可以管理规则，例如创建、显示、删除、描述、启动、停止和重新启动规则。

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

## 删除规则

该 API 用于删除规则。

```shell
DELETE http://localhost:8080/rules/{id}
```


## 启动规则

该 API 用于开始运行规则。

```shell
POST http://localhost:8080/rules/{id}/start
```


## 停止规则

该 API 用于停止运行规则。

```shell
POST http://localhost:8080/rules/{id}/stop
```

## 重启规则

该 API 用于重启规则。

```shell
POST http://localhost:8080/rules/{id}/restart
```

## 获取规则的状态

该命令用于获取规则的状态。 如果规则正在运行，则将实时检索状态指标。 状态可以是：

- $metrics
- 停止： $reason

```shell
GET http://localhost:8080/rules/{id}/status
```

响应示例：

```shell
{
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