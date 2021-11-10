# 目标 （Sink） 扩展

eKuiper 可以将数据接收到外部系统。 eKuiper具有对  [MQTT 消息服务器](../../rules/sinks/mqtt.md) 和 [日志目标](../../rules/sinks/logs.md)的内置接收器支持。然而， 仍然需要将数据发布到各种外部系统，包括消息传递系统和数据库等。Sink （目标）扩展正是为了满足这一要求。

## 开发

### 开发 Sink （目标）

为 eKuiper 开发 Sink （目标），是实现 [api.Sink](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) 接口并将其导出为 golang 插件。

在开始开发之前，您必须为 [golang 插件设置环境](../overview.md#setup-the-plugin-developing-environment)。

要开发 Sink （目标），必须实现 _Configure_ 方法。 接收器初始化后，将调用此方法。 在此方法中，将传入包含 [规则操作定义](../../rules/overview.md#actions)中的配置映射，通常，将包含诸如外部系统的主机、端口、用户和密码之类的信息。您可以使用此映射来初始化此 Sink （目标）。

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
2. Map `map[string]interface{}`: 当 [`sendSingle` 属性](../../rules/overview.md#目标/动作)设置为 true 时，可能收到此类型。

大多数时候，收到到的 map 的内容为规则选择的列的值。但是，如果 `sendError` 属性设置为 true 且规则有错误，则错误信息会放到 map 里，形似 `{"error":"error message here"}` 。

开发者可通过 context 方法`ctx.TransformOutput()` 获取转换后的字节数组。默认情况下，该方法将返回 json 编码的字节数组，若 [`dataTemlate` 属性](../../rules/overview.md#数据模板) 有设置，则返回格式化后的字符串数组，且第二个返回值设为 true，表示结果已经过变换。

需要注意的是，当 [`dataTemlate` 属性](../../rules/overview.md#数据模板) 设置时，开发者可通过 context 方法`ctx.TransformOutput()` 获取转换后的数据。若数据模板未设置，则该方法返回空值。

该方法可以返回任何错误类型。但是，如果想要让自动重试机制生效，返回的错误消息必须以 "io error" 开头。大多数情况下，也只有 io 问题才有重试的需要。

```go
//Called when each row of data has transferred to this sink
Collect(ctx StreamContext, data interface{}) error
```

最后要实现的方法是 _Close_ ，它实际上关闭了连接。 当流即将终止时调用它。 您也可以在此函数中执行任何清理工作。

```go
Close(ctx StreamContext) error
```

由于 Sink （目标）本身是一个插件，因此它必须位于主程序包中。 给定 Sink （目标）结构名称为 mySink。 在文件的最后，必须将 Sink （目标）导出为以下符号。 共有 [2种类型的导出符号](../overview.md#plugin-development)。 对于 Sink （目标）扩展，通常需要状态，因此建议导出构造函数。

```go
func MySink() api.Sink {
	return &mySink{}
}
```

[Memory Sink](https://github.com/lf-edge/ekuiper/blob/master/extensions/sinks/memory/memory.go) 是一个很好的示例。

#### 解析动态属性

在自定义的 sink 插件中，用户可能仍然想要像内置的 sink 一样支持[动态属性](../../rules/overview.md#动态属性)。 我们在 context 对象中提供了 `ParseDynamicProp` 方法使得开发者可以方便地解析动态属性并应用于插件中。开发组应当根据业务逻辑，设计那些属性支持动态值。然后在代码编写时，使用此方法解析用户传入的属性值。

```go
// Parse the prop of jsonpath syntax against the current data.
value, err := ctx.ParseDynamicProp(s.prop, data)
// Use the parsed value for the following business logic.
```

### 将 Sink （目标）打包
将实现的 Sink （目标）构建为 go 插件，并确保输出的 so 文件位于 plugins/sinks 文件夹中。

```bash
go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/sinks/MySink.so extensions/sinks/my_sink.go
```

### 使用

自定义 Sink （目标）在 [动作定义](../../rules/overview.md#actions)规定。 它的名称用作操作的键， 配置就是值。

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