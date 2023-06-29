# 规则管道

我们可以通过将先前规则的结果导入后续规则来形成规则管道。 这可以通过使用中间存储或 MQ（例如 mqtt 消息服务器）来实现。 通过同时使用 [内存源](../sources/builtin/memory.md) 和 [目标](../sinks/builtin/memory.md)，我们可以创建没有外部依赖的规则管道。

## 使用

规则管道将是隐式的。 每个规则都可以使用一个内存目标/源。 这意味着每个步骤将使用现有的 api 单独创建（示例如下所示）。

```shell
#1 创建源流
{"sql" : "create stream demo () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"}

#2 创建规则和内存目标
{
  "id": "rule1",
  "sql": "SELECT * FROM demo WHERE isNull(temperature)=false",
  "actions": [{
    "log": {
    },
    "memory": {
      "topic": "home/ch1/sensor1"
    }
  }]
}

#3 从内存主题创建一个流
{"sql" : "create stream sensor1 () WITH (DATASOURCE=\"home/+/sensor1\", FORMAT=\"JSON\", TYPE=\"memory\")"}

#4 从内存主题创建另一个要使用的规则
{
  "id": "rule2-1",
  "sql": "SELECT avg(temperature) FROM sensor1 GROUP BY CountWindow(10)",
  "actions": [{
    "log": {
    },
    "memory": {
      "topic": "analytic/sensors"
    }
  }]
}

{
  "id": "rule2-2",
  "sql": "SELECT temperature + 273.15 as k FROM sensor1",
  "actions": [{
    "log": {
    }
  }]
}

```

通过使用内存主题作为桥梁，我们现在创建一个规则管道：`rule1->{rule2-1, rule2-2}`。 管道可以是多对多的，而且非常灵活。

请注意，内存目标可以与其他目标一起使用，为一个规则创建多个规则动作。 并且内存源主题可以使用通配符订阅过滤后的主题列表。
