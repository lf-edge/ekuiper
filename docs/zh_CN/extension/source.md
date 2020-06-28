# 源（Source）扩展 

源将数据从其他系统反馈到Kuiper。 Kuiper支持 [MQTT消息服务器](../rules/sources/mqtt.md)的内置源。 然而，用户仍然需要从各种外部系统（包括消息传递系统和数据管道等）中获取数据。源扩展正是为了满足此要求。

## 开发

### 开发一个源

为Kuiper开发源的 是实现[api.Source](../../../xstream/api/stream.go)接口并将其导出为golang插件。

在开始开发之前，您必须[为golang插件设置环境](overview.md#setup-the-plugin-developing-environment)。

要开发源，必须实现 _Configure_ 方法。 初始化源后，将调用此方法。 在此方法中，您可以从第一个参数检索流的 _DATASOURCE_ 属性（这是mqtt和其他消息传递系统的主题）。 然后在第二个参数中，传递包含 _yaml_ 文件中的配置的映射。 有关更多详细信息，请参见 [配置](#deal-with-configuration)。 通常，将有外部系统的信息，例如主机、端口、用户和密码。 您可以使用此映射来初始化此源。

```go
//Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties read from the yaml 
Configure(datasource string, props map[string]interface{}) error
```

源的主要任务是实现 _open_ 方法，且应该和创建到外部系统的连接保持同步。然后从外部系统连续接收数据，并将接收到的消息发送到消费通道。消费通道接受SourceTuple接口，该接口由消息正文的映射和可选元数据的另一个映射组成。通常，使用`api.NewDefaultSourceTuple(message, meta)`命令创建SourceTuple。元数据可以是任何值得记录的内容。例如，消息的合格主题。第一个参数是StreamContext指针。您可以从中检索上下文信息和日志等。它也是go上下文的实现，因此您可以监听Done()通道以了解父流是否已退出。对于在连接或接收过程中发生的任何错误，请使用此方法进行处理。如果错误无法处理，请将其发送到errCh。默认情况下，如果从errCh收到任何错误，则该规则将终止。

```go
//Should be sync function for normal case. The container will run it in go func
Open(ctx StreamContext, consumer chan<- SourceTuple, errCh chan<- error)
```

最后要实现的方法是 _Close_ ，它实际上用来关闭连接。 当流即将终止时调用它。 您也可以在此功能中执行任何清理工作。

```go
Close(ctx StreamContext) error
```

由于源本身是一个插件，因此它必须位于主程序包中。 给定源结构名称为mySource。 在文件的最后，必须将源作为符号导出，如下所示。 有 [2种类型的导出符号](overview.md#plugin-development)。 对于源扩展，通常需要状态，因此建议导出构造函数。

```go
function MySource() api.Source{
    return &mySource{}
}
```

[Randome Source](../../../plugins/sources/random.go) 是一个很好的示例。

### 处理配置

Kuiper配置的格式为yaml，它提供了一个集中位置 _/etc_ 来保存所有配置。 在其中，为源配置提供了一个子文件夹 _sources_ ，同时也适用于扩展源。

Kuiper扩展支持配置系统自动读取yaml文件中的配置，并将其输入到源的_Configure_方法中。 如果在流中指定了[CONF_KEY](../streams.md#create-stream) 属性，则将输入该键的配置。 否则，将使用默认配置。

要在源中使用配置，必须遵循以下约定：
 1. 您的配置文件名称必须与 _.so_ 文件相同，并且必须 采用驼峰式命名法， 首字母大写。 例如，MySource.yaml。
  2. yaml文件必须位于 _etc/sources_ 内。
  3. 可以在 [此处](../rules/sources/mqtt.md)找到yaml文件的格式。

#### 通用配置字段

有两个通用配置字段。

* ``concurrency``指定将启动多少实例来运行源。
* ``bufferLength``指定要在内存中缓冲的最大消息数。 这是为了避免过多的内存使用情况而导致内存不足错误。 请注意，内存使用情况将因实际缓冲区而异。 在此处增加长度不会增加初始内存分配，因此可以安全设置较大的缓冲区长度。 默认值为102400，即如果每个消息体大小约为100个字节，则最大缓冲区大小将约为102400 * 100B〜= 10MB。

### 打包源
将已实现的源构建为go插件，并确保输出的so文件位于plugins/sources文件夹中。

```bash
go build --buildmode=plugin -o plugins/sources/MySource.so plugins/sources/my_source.go
```

### 使用

在[流定义](../streams.md#create-stream)中指定自定义源， 相关属性为：

- TYPE：指定源名称，必须为驼峰式命名。
- CONF_KEY：指定要使用的配置键。

如果您开发了源实现MySource，则应该具有：
1. 在插件文件中，将导出符号MySource。
2. 编译的MySource.so文件位于_plugins/sources_内部。
3. 如果需要配置，请将mySource.yaml放在_etc/sources_中。

要使用它，请定义一个流：
```sql
CREATE STREAM demo (
		USERID BIGINT,
		FIRST_NAME STRING,
		LAST_NAME STRING,
		NICKNAMES ARRAY(STRING),
		Gender BOOLEAN,
		ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT),
	) WITH (DATASOURCE="mytopic", TYPE="mySource", CONF_KEY="democonf");
```
