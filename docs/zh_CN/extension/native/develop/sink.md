# 目标 （Sink） 扩展

eKuiper 可以将数据接收到外部系统。 eKuiper具有对  [MQTT 消息服务器](../../../guide/sinks/builtin/mqtt.md) 和 [日志目标](../../../guide/sinks/builtin/log.md)的内置接收器支持。然而， 仍然需要将数据发布到各种外部系统，包括消息传递系统和数据库等。Sink （目标）扩展正是为了满足这一要求。

## 开发

### 开发 Sink （目标）

为 eKuiper 开发 Sink （目标），是实现 [api.Sink](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) 接口并将其导出为 golang 插件。

在开始开发之前，您必须为 [golang 插件设置环境](../overview.md#插件开发环境设置)。

要开发 Sink （目标），必须实现 _Configure_ 方法。 接收器初始化后，将调用此方法。 在此方法中，将传入包含 [规则操作定义](../../../guide/sinks/overview.md)中的配置映射，通常，将包含诸如外部系统的主机、端口、用户和密码之类的信息。您可以使用此映射来初始化此 Sink （目标）。

```go
//Called during initialization. Configure the sink with the properties from action definition 
Configure(props map[string]interface{}) error
```
下一个任务是实现 _open_ 方法。 该实现应和创建到外部系统的连接同步。 提供了上下文参数以检索上下文信息、日志和规则元信息。

```go
//Should be sync function for normal case. The container will run it in go func
Open(ctx StreamContext) error
```

Sink （目标）的主要任务是实现 _collect_ 方法。 当 eKuiper 将任何数据输入 Sink （目标）时，将调用该函数。 作为无限流，此函数将被连续调用。 此功能的任务是将数据发布到外部系统。 第一个参数是上下文，第二个参数是从 eKuiper 接收的数据。接收到的数据有2种可能的类型:
1. Map数组 `[]map[string]interface{}`: 默认类型。
2. Map `map[string]interface{}`: 当 [`sendSingle` 属性](../../../guide/sinks/overview.md#公共属性)设置为 true 时，可能收到此类型。

大多数时候，收到到的 map 的内容为规则选择的列的值。但是，如果 `sendError` 属性设置为 true 且规则有错误，则错误信息会放到 map 里，形似 `{"error":"error message here"}` 。

开发者可以使用两种方法来获取转换后的数据： context 方法`ctx.TransformOutput(data)`和来自`transform`包中的`TransItem(data, dataField, fields)`。
- `ctx.TransformOutput(data)`：
  - 参数
    - `data`: 传入的数据，类型为`interface{}`。
  - 返回值
    - 转换后的字节数组(`[]byte`)。
    - 是否转换（`bool`）。如果为`false`，表示结果没有进行转换，而是返回了原值。
    - 错误信息(`error`)。
  - 转换过程：根据`dataTemplate`、`dataField`和`fields`属性对输入数据进行转换，返回字符数组。 如果设置了[`dataTemplate` 属性](../../../guide/sinks/data_template.md)，该方法首先通过`dataTemplate` 属性得到格式化后的字符数组。如果`dataField`和`fields`都未设置，则直接输出格式化后的字符数组，否则将该字符数组转为结构化数据，再根据`dataField`和`fields`属性提取所需数据，最后再将数据编码为字节数组返回。
- `TransItem(data, dataField, fields)`：
  - 参数
    - `data`: 传入的数据，类型为`interface{}`。
    - `dataField`：指定要提取哪些数据，类型为`string`，详见[`dataField` 属性](../../../guide/sinks/overview.md#公共属性)。
    - `fields`：选择输出消息的字段，类型为`[]string`，详见[`fields` 属性](../../../guide/sinks/overview.md#公共属性)。
  - 返回值
    - 转换后的数据(`interface{}`)。
    - 是否转换（`bool`）。如果为`false`，表示结果没有进行转换，而是返回了原值。
    - 错误信息(`error`)。
  - 转换过程：`TransItem(data, dataField, fields)`根据`dataField`和`fields`属性对输入数据进行转换，返回结构化数据。如果设置了`dataField` 属性，该方法首先通过`dataField` 属性提取内嵌的数据。接着，如果设置了`fields`属性，该方法会从提取的数据中选择想要的字段。最后，转换后的数据会被返回。

该方法可以返回任何错误类型。但是，如果想要让自动重试机制生效，返回的错误消息必须以 "io error" 开头。大多数情况下，也只有 io 问题才有重试的需要。

```go
//Called when each row of data has transferred to this sink
Collect(ctx StreamContext, data interface{}) error
```

最后要实现的方法是 _Close_ ，它实际上关闭了连接。 当流即将终止时调用它。 您也可以在此函数中执行任何清理工作。

```go
Close(ctx StreamContext) error
```

由于 Sink （目标）本身是一个插件，因此它必须位于主程序包中。 给定 Sink （目标）结构名称为 mySink。 在文件的最后，必须将 Sink （目标）导出为以下符号。 共有 [2种类型的导出符号](../overview.md#插件开发)。 对于 Sink （目标）扩展，通常需要状态，因此建议导出构造函数。

```go
func MySink() api.Sink {
	return &mySink{}
}
```

[Memory Sink](https://github.com/lf-edge/ekuiper/blob/master/extensions/sinks/memory/memory.go) 是一个很好的示例。

#### 可更新的 Sink

如果你的 Sink 是可更新的，你将需要处理 `rowkindField` 属性。有些 sink 可能还需要一个 `keyField' 属性来指定哪个字段是要更新的主键。

因此，在_Configure_方法中，需要解析 `rowkindField` 以知道数据中的哪个字段表示更新的动作。然后在_Collect_方法中，通过该字段获取动作类型，并执行适当的操作。rowkind 的值可以是 `insert`、`update`、`upsert` 和 `delete`。例如，在 SQL sink 中，每种 rowkind 值将产生不同的SQL语句来执行。

#### 解析动态属性

在自定义的 sink 插件中，用户可能仍然想要像内置的 sink 一样支持[动态属性](../../../guide/sinks/overview.md#动态属性)。 我们在 context 对象中提供了 `ParseTemplate` 方法使得开发者可以方便地解析动态属性并应用于插件中。开发组应当根据业务逻辑，设计那些属性支持动态值。然后在代码编写时，使用此方法解析用户传入的属性值。

```go
// Parse the prop of go template syntax against the current data.
value, err := ctx.ParseTemplate(s.prop, data)
// Use the parsed value for the following business logic.
```

### 将 Sink （目标）打包
将实现的 Sink （目标）构建为 go 插件，并确保输出的 so 文件位于 plugins/sinks 文件夹中。

```bash
go build -trimpath --buildmode=plugin -o plugins/sinks/MySink.so extensions/sinks/my_sink.go
```

### 使用

自定义 Sink （目标）在 [动作定义](../../../guide/sinks/overview.md)规定。 它的名称用作操作的键， 配置就是值。

如果您开发了 Sink （目标）实现 MySink，则应该具有：
1. 在插件文件中，将导出符号 MySink。
2. 编译的 MySink.so 文件位于 _plugins/sinks_ 内部

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
而 _mySink_ 是动作的键。 mySink 的值是该 Sink （目标）的属性。