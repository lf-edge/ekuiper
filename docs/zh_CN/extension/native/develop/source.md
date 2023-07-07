# 源（ Source ）扩展

源将数据从其他系统反馈到 eKuiper。eKuiper 支持 [MQTT 消息服务器](../../../guide/sources/builtin/mqtt.md)的内置源。然而，用户仍然需要从各种外部系统（包括消息传递系统和数据管道等）中获取数据。源扩展正是为了满足此要求。

## 开发

有两种类型的源。一种是普通源，即扫描源（Scan Source），另一种是查询源（Lookup Source）。一个正常的源可以作为一个流或扫描表使用；一个查询源可以作为一个查询表使用。用户可以在一个源插件中开发一种或两种源。

### 开发普通源

为 eKuiper 开发源的 是实现 [api.Source](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) 接口并将其导出为 golang 插件。

在开始开发之前，您必须为 [golang 插件设置环境](../overview.md#插件开发环境设置)。

要开发源，必须实现 _Configure_ 方法。 初始化源后，将调用此方法。 在此方法中，您可以从第一个参数检索流的 _DATASOURCE_ 属性（这是 mqtt 和其他消息传递系统的主题）。 然后在第二个参数中，传递包含 _yaml_ 文件中的配置的映射。 有关更多详细信息，请参见 [配置](#处理配置)。 通常，将有外部系统的信息，例如主机、端口、用户和密码。 您可以使用此映射来初始化此源。

```go
//在初始化期间调用。 使用数据源（例如，mqtt 的主题）和从 Yaml 读取的属性来配置源 
Configure(datasource string, props map[string]interface{}) error
```

源的主要任务是实现 _open_ 方法，且应该和创建到外部系统的连接保持同步。然后从外部系统连续接收数据，并将接收到的消息发送到消费通道。消费通道接受 SourceTuple 接口，该接口由消息正文的映射和可选元数据的另一个映射组成。有两种方法可用于帮助开发人员创建`SourceTuple`对象： `api.NewDefaultSourceTuple(message, meta)` 和 `api.NewDefaultSourceTupleWithTime(message, meta, time)`，这两个方法的不同之处在于，前者创建的SourceTuple对象的时间戳是在函数调用时生成的，而后者创建的SourceTuple对象的时间戳可以由用户在调用时指定。元数据可以是任何值得记录的内容。例如，消息的合格主题。第一个参数是 StreamContext 指针。您可以从中检索上下文信息和日志等。它也是 go 上下文的实现，因此您可以监听 Done() 通道以了解父流是否已退出。对于在连接或接收过程中发生的任何错误，请使用此方法进行处理。如果错误无法处理，请将其发送到 errCh。默认情况下，如果从 errCh 收到任何错误，则该规则将终止。

```go
//Should be sync function for normal case. The container will run it in go func
Open(ctx StreamContext, consumer chan<- SourceTuple, errCh chan<- error)
```

最后要实现的方法是 _Close_，它实际上用来关闭连接。 当流即将终止时调用它。 您也可以在此功能中执行任何清理工作。

```go
Close(ctx StreamContext) error
```

由于源本身是一个插件，因此它必须位于主程序包中。 给定源结构名称为 mySource。 在文件的最后，必须将源作为符号导出，如下所示。 有 [2种类型的导出符号](../overview.md#插件开发)。 对于源扩展，通常需要状态，因此建议导出构造函数。

```go
function MySource() api.Source{
    return &mySource{}
}
```

[Random Source](https://github.com/lf-edge/ekuiper/blob/master/extensions/sources/random/random.go)  是一个很好的示例。

### 开发查询源

为 eKuiper 开发一个查询源就是实现 [api.LookupSource](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) 接口并将其导出。

在开始开发之前，您必须为 [golang 插件设置环境](../overview.md#插件开发环境设置)。

要开发一个查询源，必须实现 _Configure_ 方法。 初始化源后，将调用此方法。 在此方法中，您可以从第一个参数检索流的 _DATASOURCE_ 属性（这是 mqtt 和其他消息传递系统的主题）。 然后在第二个参数中，传递包含 _yaml_ 文件中的配置的映射。 有关更多详细信息，请参见 [配置](#处理配置)。 通常，将有外部系统的信息，例如主机、端口、用户和密码。 您可以使用此映射来初始化此源。

```go
//在初始化过程中调用。用数据源（例如mqtt的topic）和从yaml中读取的属性来配置这个源 
Configure(datasource string, props map[string]interface{}) error
```

下一个任务是实现 _open_ 方法。一旦源被创建，该方法将被调用。它负责初始化，比如建立连接。

```go
// Open 创建与外部数据源的连接
Open(ctx StreamContext) error
```

查询源的主要任务是实现 _Lookup_ 方法。该方法将在每个连接操作中运行。参数是在运行时获得的，包括要从外部系统中检索的字段、键和值等信息。每个查询源都有不同的查询机制。例如，SQL 查询源将从这些参数中组装一个 SQL 查询来检索查询数据。

```go
// Lookup 接收查询值以构建查询并返回查询结果
Lookup(ctx StreamContext, fields []string, keys []string, values []interface{}) ([]SourceTuple, error)
```  

最后要实现的方法是 _Close_，它实际上用来关闭连接。当流即将终止时调用它。 您也可以在此功能中执行任何清理工作。

```go
Close(ctx StreamContext) error
```

由于源本身是一个插件，因此它必须位于主程序包中。Export 的名称必须以 `Lookup` 结尾，这样它就可以被称为 `MySource` 的查询源。对于源扩展，通常需要状态，所以建议导出一个构造函数。

```go
function MySourceLookup() api.LookupSource{
    return &mySource{}。
}
```

[SQL Lookup Source](https://github.com/lf-edge/ekuiper/blob/master/extensions/sources/sql/sqlLookup.go) 是一个很好的示例。

### 可回溯源

如果[规则检查点](../../../guide/rules/state_and_fault_tolerance.md#源考虑)被启用，源需要可回退。这意味着源需要同时实现 `api.Source` 和 `api.Rewindable` 接口。

一个典型的实现是将 "offset" 作为源的一个字段来保存。当读入新的值时更新偏移值。注意，当实现 GetOffset() 时，将被 eKuiper 系统调用，这意味着偏移值可以被多个 go routines 访问。因此，在读或写偏移量时，需要一个锁。

### 处理配置

eKuiper 配置的格式为 yaml，它提供了一个集中位置  _/etc_  来保存所有配置。 在其中，为源配置提供了一个子文件夹  _sources_，同时也适用于扩展源。

eKuiper 扩展支持配置系统自动读取 yaml 文件中的配置，并将其输入到源的 _Configure_ 方法中。 如果在流中指定了 [CONF_KEY](../../../guide/streams/overview.md#流属性)  属性，则将输入该键的配置。 否则，将使用默认配置。

要在源中使用配置，必须遵循以下约定：

 1. 您的配置文件名称必须与插件名字相同，例如，mySource.yaml。
 2. yaml 文件必须位于 _etc/sources_ 内。
 3. 可以在[此处](../../../guide/sources/builtin/mqtt.md)找到 yaml 文件的格式。

#### 通用配置字段

有两个通用配置字段。

* `concurrency` 指定将启动多少实例来运行源。
* `bufferLength` 指定要在内存中缓冲的最大消息数。 这是为了避免过多的内存使用情况而导致内存不足错误。 请注意，内存使用情况将因实际缓冲区而异。 在此处增加长度不会增加初始内存分配，因此可以安全设置较大的缓冲区长度。 默认值为102400，即如果每个消息体大小约为100个字节，则最大缓冲区大小将约为102400 * 100B〜= 10MB。

### 打包源

将已实现的源构建为 go 插件，并确保输出的 so 文件位于 plugins/sources 文件夹中。

```bash
go build -trimpath --buildmode=plugin -o plugins/sources/MySource.so extensions/sources/my_source.go
```

### 使用

在[流定义](../../../guide/streams/overview.md#流属性)中指定自定义源， 相关属性为：

* TYPE：指定源名称，必须为驼峰式命名。
* CONF_KEY：指定要使用的配置键。

如果您开发了源实现 MySource，则应该具有：

1. 在插件文件中，将导出符号 MySource。
2. 编译的 MySource.so 文件位于 _plugins/sources_ 内部。
3. 如果需要配置，请将 mySource.yaml 放在 _etc/sources_ 中。

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
