# 规则管理

Kuiper规则命令行工具可以管理规则，例如创建、显示、删除、描述、启动、停止和重新启动规则。

## 创建规则

如下命令用于创建规则。 规则的定义以JSON 格式指定，请阅读 [rule](../rules/overview.md) 以获取更多详细信息。

```shell
create rule $rule_name $rule_json | create rule $rule_name -f $rule_def_file
```

可以通过两种方式创建规则。

- 在命令行中指定规则定义。

示例：

```shell
# bin/cli create rule rule1 {"sql": "SELECT * from demo","actions": [{"log":  {}},{"mqtt":  {"server":"tcp://127.0.0.1:1883", "topic":"demoSink"}}]}
```

该命令创建一个名为``rule1``的规则。

- 在文件中明确规则定义。 如果规则很复杂，或者规则已经以组织良好的格式写在文本文件中，则只需通过-f选项指定规则定义即可。

示例：

```shell
# bin/cli create rule rule1 -f /tmp/rule.txt
```

以下是`rule.txt`的内容。

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

## 展示规则

该命令用于显示服务器中定义的所有规则，包括规则id和当前状态。

```shell
show rules
```

示例：

```shell
# bin/cli show rules
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

该命令用于打印规则的详细定义。

```shell
describe rule $rule_name
```

示例：

```shell
# bin/cli describe rule rule1
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

该命令用于删除规则。

```shell
drop rule $rule_name
```

示例：

```shell
# bin/cli drop rule rule1
rule rule1 dropped
```

## 启动规则

该命令用于开始运行规则。

```shell
start rule $rule_name
```

示例：

```shell
# bin/cli start rule rule1
rule rule1 started
```

## 停止规则

该命令用于停止运行规则。

```shell
stop rule $rule_name
```

示例：

```shell
# bin/cli stop rule rule1
rule rule1 stopped
```

## 重启规则

该命令用于重启规则。

```shell
restart rule $rule_name
```

示例：

```shell
# bin/cli restart rule rule1
rule rule1 restarted
```

## 获取规则的状态

该命令用于获取规则的状态。 状态可以是
- 运行: $metrics
- 停止: $reason

```shell
getstatus rule $rule_name
```

示例：

```shell
# bin/cli getstatus rule rule1
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