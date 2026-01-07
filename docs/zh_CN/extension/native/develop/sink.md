# 目标 （Sink） 扩展

eKuiper 可以将数据接收到外部系统。 eKuiper具有对 [MQTT 消息服务器](../../../guide/sinks/builtin/mqtt.md)
和 [日志目标](../../../guide/sinks/builtin/log.md)等内置接收器支持。然而，仍然需要将数据发布到各种外部系统，包括消息传递系统和数据库等。Sink（目标）扩展正是为了满足这一要求。

**_请注意_**：v2.0.0 修改了 sink 扩展 API，与 v1.x 的插件 API 不完全兼容。原有的插件代码需要重新适配。

## 开发

为 eKuiper 开发 Sink（目标），是实现 [api.Sink](https://github.com/lf-edge/ekuiper/blob/master/contract/api/sink.go)
接口并将其导出为 Go 插件。

在开始开发之前，您必须为 [Go 插件设置环境](./overview.md#插件开发环境设置)。

根据 Sink 需要处理的数据是否为二进制数据，Sink 可分为 2 类接口：

- BytesCollector: 接收框架编码后的二进制数据，例如 MQTT Sink。
- TupleCollector：接收结构化的 map 数据，插件需要自行编码。例如 SQL Sink。

要开发 Sink，首先需要确认扩展的 Sink 属于哪个类型，然后实现对应类型的方法。

### 通用方法

所有 Sink 都需要实现以下通用方法：

1. 要开发 Sink （目标），必须实现 **Provision** 方法。 接收器初始化后，将调用此方法。
   在此方法中，将传入包含 [规则操作定义](../../../guide/sinks/overview.md)
   中的配置映射，通常，将包含诸如外部系统的主机、端口、用户和密码之类的信息。您可以使用此映射来初始化此 Sink（目标）。

   ```go
   //在初始化期间调用。 用于读取用户配置，初始化数据源
   Provision(ctx StreamContext, configs map[string]any) error
   ```

2. 实现 **Connect**
   方法。该方法用于初始化建立与外部系统的连接，仅在规则初始化时执行一次。其中，第二个参数用于传递长连接状态给规则。例如，当连接实现会自动重连，重连逻辑应当为异步运行，以免阻塞规则运行。连接逻辑变为异步运行，连接状态变更可通过调用状态变化回调函数通知规则。

   ```go
   //在初始化期间调用。 用于初始化外部连接。
   Connect(ctx StreamContext, sch StatusChangeHandler) error
   ```

3. 实现 Sink 类型对应的 Collect 方法。这是 Sink 的主要执行逻辑，用于发送数据到外部系统。作为无限流，此函数将被连续调用。不同类型的
   Sink 实现的方法略有不同，详情请看[Sink 类型实现](#sink-类型实现)。

4. 最后要实现的方法是 **Close**，它实际上用来关闭连接。 当流即将终止时调用它。 您也可以在此功能中执行任何清理工作。

   ```go
   Close(ctx StreamContext) error
   ```

5. 导出符号，给定源结构名称为 mySink。 在文件的最后，必须将源作为符号导出，如下所示。
   有 [2种类型的导出符号](./overview.md#插件开发)。 对于源扩展，通常需要状态，因此建议导出构造函数。

   ```go
   func MySink() api.Sink {
       return &mySink{}
   }
   ```

[Memory Sink](https://github.com/lf-edge/ekuiper/blob/master/internal/io/memory/sink.go) 是一个很好的示例。

### Sink 类型实现

根据发送的数据类型的不同，Sink 的类型可分为两类，用户可分别实现不同的 Collect 方法。

- BytesCollector: 实现 Collect 方法，处理上游算子发送过来的 RawTuple。用户可通过 `RawTuple.Raw()` 获取编码后的二进制数据进行处理。可参考
  MQTT Sink 实现。

  ```go
  Collect(ctx StreamContext, item RawTuple) error
  ```

- TupleCollector: 实现 Collect 和 CollectList 方法，处理上游算子发送过来的 Tuple 或者 Tuple List。可参考 SQL Sink 实现。

  ```go
  Collect(ctx StreamContext, item MessageTuple) error
  CollectList(ctx StreamContext, items MessageTupleList) error
  ```

Collect 方法实现可以返回任何错误类型。但是，如果想要让自动重试机制生效，返回的错误消息必须以 "io error" 开头。大多数情况下，也只有
io 问题才有重试的需要。

### 解析动态属性

在自定义的 sink 插件中，用户可能仍然想要像内置的 sink 一样支持[动态属性](../../../guide/sinks/overview.md#动态属性)。
Collect 方法传入的 Tuple 中包含解析后的动态值。开发组应当根据业务逻辑，设计哪些属性支持动态值。然后在代码编写时，使用如下方法解析用户传入的属性值。

```go
func Collect(ctx StreamContext, item RawTuple) error {
    if dp, ok := item.(api.HasDynamicProps); ok {
        temp, transformed := dp.DynamicProps("propName")
        if transformed {
            tpc = temp
        }
    }
}
```

## 使用

自定义 Sink （目标）在 [动作定义](../../../guide/sinks/overview.md)规定。 它的名称用作操作的键， 配置就是值。

如果您开发了 Sink （目标）实现 MySink，则应该具有：

1. 在插件文件中，将导出符号 MySink。
2. 编译的 MySink.so 文件位于 **plugins/sinks** 内部

要使用它，请在规则定义内定义动作 mySink：

```json
{
  "id": "rule1",
  "sql": "SELECT demo.temperature, demo1.temp FROM demo left join demo1 on demo.timestamp = demo1.timestamp where demo.temperature > demo1.temp GROUP BY demo.temperature, HOPPINGWINDOW(ss, 20, 10)",
  "actions": [
    {
      "mySink": {
        "server": "tcp://47.52.67.87:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```

而 **mySink** 是动作的键。 mySink 的值是该 Sink （目标）的属性。
