# Sink（目标） 扩展

Kuiper可以将数据接收到外部系统。 Kuiper具有对 [MQTT消息服务器](../rules/sinks/mqtt.md) 和 [日志接收器](../rules/sinks/logs.md)的内置接收器支持。然而， 仍然需要将数据发布到各种外部系统，包括消息传递系统和数据库等。Sink（目标）扩展正是为了满足这一要求。

## 开发

### 开发Sink（目标）

为Kuiper开发Sink（目标），是实现[api.Sink](../../../xstream/api/stream.go) 接口并将其导出为golang插件。

在开始开发之前，您必须[为golang插件设置环境](overview.md#setup-the-plugin-developing-environment)。

要开发Sink（目标），必须实现_Configure_方法。 接收器初始化后，将调用此方法。 在此方法中，将传入包含 [规则操作定义](../rules/overview.md#actions)中的配置映射，通常，将包含诸如外部系统的主机、端口、用户和密码之类的信息。您可以使用此映射来初始化此Sink（目标）。

```go
//Called during initialization. Configure the sink with the properties from action definition 
Configure(props map[string]interface{}) error
```
下一个任务是实现_open_方法。 该实现应和创建到外部系统的连接同步。 提供了上下文参数以检索上下文信息、日志和规则元信息。

```go
//Should be sync function for normal case. The container will run it in go func
Open(ctx StreamContext) error
```

Sink（目标）的主要任务是实现_collect_方法。 当Kuiper将任何数据输入Sink（目标）时，将调用该函数。 作为无限流，此函数将被连续调用。 此功能的任务是将数据发布到外部系统。 第一个参数是上下文，第二个参数是从Kuiper接收的数据。

```go
//Called when each row of data has transferred to this sink
Collect(ctx StreamContext, data interface{}) error
```

最后要实现的方法是_Close_，它实际上关闭了连接。 当流即将终止时调用它。 您也可以在此函数中执行任何清理工作。

```go
Close(ctx StreamContext) error
```

由于Sink（目标）本身是一个插件，因此它必须位于主程序包中。 给定Sink（目标）结构名称为mySink。 在文件的最后，必须将Sink（目标）导出为以下符号。 共有 [2种类型的导出符号](overview.md#plugin-development)。 对于Sink（目标）扩展，通常需要状态，因此建议导出构造函数。

```go
func MySink() api.Sink {
	return &mySink{}
}
```

[Memory Sink](../../../plugins/sinks/memory.go) 是一个很好的示例。

### 将Sink（目标）打包
将实现的Sink（目标）构建为go插件，并确保输出的so文件位于plugins/sinks文件夹中。

```bash
go build --buildmode=plugin -o plugins/sinks/MySink.so plugins/sinks/my_sink.go
```

### 使用

自定义Sink（目标）在 [动作定义](../rules/overview.md#actions)。 它的名称用作操作的键， 配置就是值。

如果您开发了Sink（目标）实现MySink，则应该具有：
1. 在插件文件中，将导出符号MySink。
2. 编译的MySink.so文件位于_plugins/sinks_内部

要使用它，请在规则定义内定义动作mySink：

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
而_mySink_是动作的键。 mySink的值是该Sink（目标）的属性。