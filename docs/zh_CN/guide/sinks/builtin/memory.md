# 内存动作

<span style="background:green;color:white">updatable</span>

该动作用于将结果刷新到内存中的主题中，以便 [内存源](../../sources/builtin/memory.md) 可以使用它。 该主题类似于 pubsub 主题，例如 mqtt，因此可能有多个内存目标发布到同一主题，也可能有多个内存源订阅同一主题。 内存动作的典型用途是形成[规则管道](../../rules/rule_pipeline.md)。

| 属性名称         | 是否可选 | 描述                                     |
|--------------|------|----------------------------------------|
| topic        | 否    | 内存中的主题，例如 `analysis/result`, 支持动态属性    |
| rowkindField | 是    | 指定哪个字段表示操作，例如插入或更新。如果不指定，默认所有的数据都是插入操作 |

下面是一个内存动作配置示例：

```json
{
  "memory": {
    "topic": "devices/result"
  }
}
```

下面是动态主题示例：

```json
{
  "memory": {
    "topic": "{{.topic}}"
  }
}
```

## 数据模板

::: v-pre
内存动作和内存源之间的数据传输采用内部格式，不经过编解码以提高效率。因此，内存动作的格式相关配置项，除了数据模板之外都会被忽略。内存动作可支持数据模板对结果格式进行变化，但是数据模板的结果必须为 JSON 字符串的 object 形式，例如 `"{\"key\":\"{{.key}}\"}"`。数组形式的 JSON 字符串或者非 JSON 字符串都不支持。
:::

## 更新

内存动作支持[更新](../overview.md#更新)。可用于更新订阅了与 sink 相同的主题的查询表。一个典型的用法是创建一个规则，使用可更新的 sink 来累积更新内存表。在下面的例子中，来自流alertStream的数据将更新内存主题`alertVal`。更新动作是由流入的数据中的 `action` 字段指定的。

```json
{
  "id": "ruleUpdateAlert",
  "sql":"SELECT * FROM alertStream",
  "actions":[
    {
      "memory": {
        "keyField": "id",
        "rowkindField": "action",
        "topic": "alertVal",
        "sendSingle": true
      }
    }
  ]
}
```
