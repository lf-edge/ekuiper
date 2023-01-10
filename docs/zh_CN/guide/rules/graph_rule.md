# 图规则

最初，eKuiper 利用 SQL 来定义规则逻辑。虽然这对开发人员来说很方便，但对没有开发知识的用户来说，还是不容易使用。即使是用SQL定义的，在运行时，规则都是一个元素的有向无环图（Source/Operator/Sink）。该图可以很容易地映射到一个拖放用户界面，以方便用户。因此，在规则API中提供了一个替代的 `graph` 属性。

`graph` 属性是有向无环图的 JSON 表述。它由 `nodes` 和 `topo` 组成，分别定义了图中的节点和它们的边。下面是一个由图形定义的最简单的规则。它定义了3个节点：`demo`，`humidityFilter` 和 `mqttOut`。这个图是线性的，即`demo`->`humidityFilter`->`mqttOut`。该规则将从mqtt(`demo`)读取，通过湿度过滤(`humidityFilter`)并汇入mqtt(`mqttOut`)。

```json
{
  "id": "rule1",
  "name": "Test Condition",
  "graph": {
    "nodes": {
      "demo": {
        "type": "source",
        "nodeType": "mqtt",
        "props": {
          "datasource": "devices/+/messages"
        }
      },
      "humidityFilter": {
        "type": "operator",
        "nodeType": "filter",
        "props": {
          "expr": "humidity > 30"
        }
      },
      "mqttout": {
        "type": "sink",
        "nodeType": "mqtt",
        "props": {
          "server": "tcp://${mqtt_srv}:1883",
          "topic": "devices/result"
        }
      }
    },
    "topo": {
      "sources": ["demo"],
      "edges": {
        "demo": ["humidityFilter"],
        "humidityFilter": ["mqttout"]
      }
    }
  }
}
```

## 节点

图的 JSON 中的每个节点至少有3个字段：

- type：节点的类型，可以是`source`、`operator`和`sink`。
- nodeType：节点的实现类型，定义了节点的业务逻辑，包括内置类型和由插件定义的扩展类型。
- props：节点的属性。它对每个 nodeType 都是不同的。

### 节点类型

对于源节点，nodeType是源的类型，如 `mqtt` 和 `edgex` 。请参考 [source](../sources/overview.md) 了解所有支持的类型。注意，所有源节点共享相同的属性，这与[定义流](../../sqls/streams.md)时的属性相同。具体的配置是由 `CONF_KEY` 定义的。在下面的例子中，nodeType 指定了源节点是一个 mqtt 源。dataSource 和 format 属性与定义流时的含义相同。

```json
  {
    "type": "source",
    "nodeType": "mqtt",
    "props": {
      "datasource": "devices/+/messages",
      "format":"json"
    }
  }
```

对于 sink 节点，nodeType 是 sink 的类型，如 `mqtt` 和 `edgex` 。请参考 [sink](../sinks/overview.md) 了解所有支持的类型。对于所有的 sink 节点，它们共享一些共同的属性，但每种类型都会有一些自有的属性。

对于 operator 节点，nodeType 是新定义的，而且每个 nodeType 有不同的属性。

### 内置 operator 节点类型

目前，我们支持以下节点类型的运算符类型。

#### 函数

这个节点定义了一个函数调用表达式。该节点返回一个新的字段，该字段带有函数的名称或expr属性中定义的别名。它只有一个属性：

- expr：字符串类型，函数调用表达式。

示例：

```json
  {
    "type": "operator",
    "nodeType": "function",
    "props": {
      "expr": "log(temperature) as log_temperature"
    }
  }
```
#### aggfunc

这个节点定义了一个聚合函数调用表达式。该节点的输入必须是一个行的集合，例如一个窗口。该节点将把多条行聚合成一条聚合的行。例如，计算窗口中10行的计数将只产生一条字段 `count`=10 的行。计算分组的行的计数将为每组产生一行。它只有一个属性：

- expr：字符串类型，聚合函数调用表达式。

示例：

```json
  {
    "type": "operator",
    "nodeType": "aggfunc",
    "props": {
      "expr": "count(*)"
    }
  }
```

#### 过滤

这个节点用一个条件表达式过滤数据流。它只有一个属性：

- expr: 字符串类型，过滤条件的布尔表达式。

示例：

```json
  {
    "type": "operator",
    "nodeType": "filter",
    "props": {
      "expr": "temperature > 20"
    }
  }
```

#### pick

这个节点选择要在接下来的流中呈现的字段。它通常用在流程的最后，以定义要选择的数据。它只有一个属性：

- fields: 字符串数组类型，定义要选择的字段

示例：

```json
  {
    "type": "operator",
    "nodeType": "pick",
    "props": {
      "fields": ["log_temperature", "humidity", "window_end()"]
    }
  }
```

#### 窗口

这个节点在工作流中定义了一个[窗口](../../sqls/windows.md)。它可以接受多个输入，但每个输入必须是一个单行。它将产生一个行的集合。

- type：字符串类型，表示窗口类型，可用值为 "tumblingwindow"、"hoppingwindow"、"slidingwindow"、"sessionwindow "和 "countwindow"。
- unit：要使用的时间单位。查看[时间单位](../../sqls/windows.md#时间单位)的所有可用值。
- size：int 类型，窗口的长度。
- interval：int 类型，窗口的触发间隔。

示例：

```json
  {
    "type": "operator",
    "nodeType": "window",
    "props": {
      "type": "hoppingwindow",
      "unit": "ss",
      "size": 10,
      "interval": 5
    }
  }
```

#### join

这个节点可以像SQL连接操作一样合并来自不同来源的数据。输入必须是一个由窗口产生的行集合。输出是另一个行集合，其行是连接的数据。其属性为：

- from：字符串类型，要连接的左边源节点。
- joins：一个连接条件的数组。每个连接都有以下属性。
  - name: 字符串类型，要连接的右边源节点
  - type：字符串类型，连接类型，可以是 inner, left, right, full, cross等。
  - on：字符串，用于定义连接条件的bool表达式。

示例：

```json
  {
    "type": "operator",
    "nodeType": "join",
    "props": {
      "from": "device1",
      "joins": [
        {
          "name": "device2",
          "type": "inner",
          "on": "abs(device1.ts - device2.ts) < 200"
        }
      ]
    }
  }
```

#### groupby

这个节点定义了要分组的维度。输入必须是一个行的集合。输出是一个分组数据的集合。其属性为：

- dimensions：字符串数组，维度的列表

示例：

```json
  {
    "type": "operator",
    "nodeType": "groupby",
    "props": {
      "dimensions": ["device1.humidity"]
    }
  },
```

#### orderby

这个节点将对输入集合进行排序。因此，输入必须是一个行的集合，输出将是相同的类型。其属性为：

- sorts: 一个排序条件的数组。每个条件都有以下属性：
  - field：字符串类型，要被排序的字段。
  - order：字符串类型，排序的方向，可以是 asc 或 desc。

示例：

```json
  {
    "type": "operator",
    "nodeType": "orderby",
    "props": {
      "sorts": [{
        "field": "count",
        "order": "desc"
      }]
    }
  }
```

#### switch

该节点允许消息被路由到不同的流程分支，类似于编程语言中的 switch 语句。目前，这是唯一有多个输出路径的节点。节点接受多个条件表达式作为评估条件，并针对评估结果路由数据。其属性如下。

- cases：要依次评估的条件表达式。
- stopAtFirstMatch：是否在匹配任何条件时停止评估，类似于编程语言中的 break。

示例:

```json
    {
      "type": "operator",
      "nodeType": "switch",
      "props": {
        "cases": [
          "temperature > 20",
          "temperature <= 20"
        ],
        "stopAtFirstMatch": true
      }
    }
```

#### script

该节点允许针对传递的信息运行 JavaScript 代码。

- script：要运行的内联JavaScript代码。
- isAgg：该节点是否用于聚合数据。

脚本中必须有一个名为 `exec` 的函数。如果 isAgg 为 false，脚本节点可以接受一个单一的消息，并且必须返回一个处理过的消息。如果 isAgg 为 true，它将接收一个消息数组（窗口输出等），并且必须返回一个数组。

1. 处理单个消息的脚本节点示例
   ```json
   {
     "type": "operator",
      "nodeType": "script",
      "props": {
        "script": "function exec(msg, meta) {msg.temperature = 1.8 * msg.temperature + 32; return msg;}"
      }
   }
   ```
2. 处理聚合消息的脚本节点示例
   ```json
   {
      "type": "operator",
      "nodeType": "script",
      "props": {
        "script": "function exec(msgs) {agg = {value:0}\nfor (let i = 0; i < msgs.length; i++) {\nagg.value = agg.value + msgs[i].value;\n}\nreturn agg;\n}",
        "isAgg": true
      }
   }
   ```