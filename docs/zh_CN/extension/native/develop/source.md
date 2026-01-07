# 源（ Source ）扩展

源将数据从其他系统反馈到 eKuiper。eKuiper 支持 [MQTT 消息服务器](../../../guide/sources/builtin/mqtt.md)
等内置源。然而，用户仍然需要从各种外部系统（包括消息传递系统和数据管道等）中获取数据。源扩展正是为了满足此要求。

**_请注意_**：v2.0.0 修改了 source 扩展 API，与 v1.x 的插件 API 不完全兼容。原有的插件代码需要重新适配。

有两种类型的源。一种是普通源，即扫描源（Scan Source），另一种是查询源（Lookup Source）。一个正常的源可以作为一个流或扫描表使用；一个查询源可以作为一个查询表使用。用户可以在一个源插件中开发一种或两种源。

## 开发普通源

为 eKuiper 开发源的 是实现 [api.Source](https://github.com/lf-edge/ekuiper/blob/master/contract/api/source.go) 接口并将其导出为
Go 插件。

在开始开发之前，您必须为 [golang 插件设置环境](./overview.md#插件开发环境设置)。

根据数据源的是否为定时拉取，数据是否为二进制数据，源可分为 4 类接口：

- ByteSource: 推送源，其 payload 为二进制数据，可配置 format 进行解码，例如 MQTT 数据源。
- TupleSource：推送源，其 payload 为非通用格式，需要插件自行解码，例如 Memory 数据源。
- PullBytesSource：拉取源，其 payload 为二进制格式，可配置 format 进行解码，例如 Video 数据源。
- PullTupleSource：拉取源，其 payload 为非通用格式，需要插件自行解码，例如 HttpPull 数据源。

要开发源，首先需要确认扩展的源属于哪个类型，然后实现对应类型的方法。

### 通用方法

所有源都需要实现以下通用方法：

1. 必须实现 **Provision** 方法。 初始化源后，将调用此方法。 在此方法中，您可以从第一个参数获取 context，进行日志书写等。
   在第二个参数中，传递包含 **yaml** 文件中的配置的映射。 有关更多详细信息，请参见 [配置](#处理配置)。
   通常，将有外部系统的信息，例如主机、端口、用户和密码。 您可以使用此映射来初始化此源。

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

3. 实现源类型的订阅或拉取方法。这是源的主要执行逻辑，用于从外部系统获取数据并发送到 eKuiper
   系统中供下游算子消费。不同类型的源实现的方法略有不同，详情请看[源类型实现](#源类型实现)。

4. 最后要实现的方法是 **Close**，它实际上用来关闭连接。 当流即将终止时调用它。 您也可以在此功能中执行任何清理工作。

   ```go
   Close(ctx StreamContext) error
   ```

5. 导出符号，给定源结构名称为 mySource。 在文件的最后，必须将源作为符号导出，如下所示。
   有 [2种类型的导出符号](../overview.md#插件开发)。 对于源扩展，通常需要状态，因此建议导出构造函数。

   ```go
   function MySource() api.Source{
       return &mySource{}
   }
   ```

[Random Source](https://github.com/lf-edge/ekuiper/blob/master/extensions/impl/random/random.go) 是一个很好的示例。

### 源类型实现

源的主要任务是从外部系统连续接收数据，读取到系统中。

- ByteSource：需要实现 Subscribe 方法，用于订阅数据变化（接收外部系统推送数据）。调用 BytesIngest 消费订阅到的数据，调用
  ErrorIngest 发送错误信息。参考 MQTT source实现，订阅配置的主题，并通过 ingest 方法读取订阅到的 bytes 数据。

  ```go
  Subscribe(ctx StreamContext, ingest BytesIngest, ingestError ErrorIngest) error
  ```

- TupleSource：需要实现 Subscribe 方法，用于订阅数据变化（接收外部系统推送数据）。调用 TupleIngest 消费订阅到并解码为 map
  的数据；调用 ErrorIngest 发送错误信息。参考 Memory source实现。

  ```go
  Subscribe(ctx StreamContext, ingest TupleIngest, ingestError ErrorIngest) error
  ```

- PullBytesSource：需要实现 Pull 方法，用于拉取数据。拉取间隔可通过 interval 参数配置。调用 BytesIngest 消费拉取到的数据，调用
  ErrorIngest 发送错误信息，trigger 为此次拉取的时间。参考 Video 数据源实现。

  ```go
  Pull(ctx StreamContext, trigger time.Time, ingest BytesIngest, ingestError ErrorIngest)
  ```

- PullTupleSource：需要实现 Pull 方法，用于拉取数据。拉取间隔可通过 interval 参数配置。调用 TupleIngest 消费拉取变解码为 map
  的数据，调用 ErrorIngest 发送错误信息，trigger 为此次拉取的时间。参考 HttpPull 数据源实现。

```go
Pull(ctx StreamContext, trigger time.Time, ingest TupleIngest, ingestError ErrorIngest)
```

## 开发查询源

为 eKuiper 开发一个查询源就是实现 Lookup 接口并将其导出。根据数据源的数据是否为二进制数据，源可分为 2 类接口：

- LookupBytesSource
- LookupSource

用户需要根据扩展的实际类型，选择一种接口实现。查询源与普通的数据源一样，需要实现[通用方法](#通用方法)。然后再实现 Lookup 方法。

查询源的主要任务是实现 **Lookup** 方法。该方法将在每个连接操作中运行。参数是在运行时获得的，包括要从外部系统中检索的字段、键和值等信息。每个查询源都有不同的查询机制。例如，SQL
查询源将从这些参数中组装一个 SQL 查询来检索查询数据。

根据 Payload 的类型，两种接口的 Lookup 方法略有不同。

- LookupSource: 插件中实现解码，返回值为 map 的列表

  ```go
  // Lookup 接收查询值以构建查询并返回查询结果
  Lookup(ctx StreamContext, fields []string, keys []string, values []any) ([]map[string]any, error)
  ```

- LookupBytesSource: 插件返回原始二进制数据，由 eKuiper 框架根据 format 参数自动解码。

```go
// Lookup 接收查询值以构建查询并返回二进制查询结果
Lookup(ctx StreamContext, fields []string, keys []string, values []any) ([][]byte, error)
```

[SQL Lookup Source](https://github.com/lf-edge/ekuiper/blob/master/extensions/impl/sql/lookupSource.go) 是一个很好的示例。

## 源特性支持

除了默认接口中的数据读入功能之外，扩展源可以选择性地实现特性接口以支持各种源特性。

### 可回溯源

如果[规则检查点](../../../guide/rules/state_and_fault_tolerance.md#源考虑)被启用，源需要可回退。这意味着源需要同时实现 `api.Source` 和 `api.Rewindable` 接口。

一个典型的实现是将 "offset" 作为源的一个字段来保存。当读入新的值时更新偏移值。注意，当实现 GetOffset() 时，将被 eKuiper 系统调用，这意味着偏移值可以被多个 go routines 访问。因此，在读或写偏移量时，需要一个锁。

### 有界源

有些数据源是有界的，例如文件；有些数据源本身是无界的，但在某些场景用户希望能够在读取一定数据后停止。eKuiper
支持数据源自定义读取结束信号。当框架收到数据结束信号时，对应的规则会自行停止。

数据源可以实现 `api.Bounded` 接口以获取 `EOFIngest` 方法。在数据读取结束后，调用该方法通知框架数据读取已完成。File Source
是内置的有界源，开发时可参考其实现。

## 配置与使用

eKuiper 配置的格式为 yaml，它提供了一个集中位置 **/etc** 来保存所有配置。 在其中，为源配置提供了一个子文件夹 **sources**
，同时也适用于扩展源。

eKuiper 扩展支持配置系统自动读取 yaml 文件中的配置，并将其输入到源的 **Provision** 方法中。
如果在流中指定了 [CONF_KEY](../../../guide/streams/overview.md#流属性) 属性，则将输入该键的配置。 否则，将使用默认配置。

要在源中使用配置，必须遵循以下约定：

1. 您的配置文件名称必须与插件名字相同，例如，mySource.yaml。
2. yaml 文件必须位于 **etc/sources** 内。
3. 可以在[此处](../../../guide/sources/builtin/mqtt.md)找到 yaml 文件的格式。

### 通用配置字段

有两个通用配置字段。

- `interval` 若数据源为拉取源类型，该参数指定拉取的间隔。若为推送源，该参数默认不配置，数据源为数据触发；若有配置，该参数会定义推送的频率。
- `bufferLength` 指定要在内存中缓冲的最大消息数。 这是为了避免过多的内存使用情况而导致内存不足错误。 请注意，内存使用情况将因实际缓冲区而异。
  在此处增加长度不会增加初始内存分配，因此可以安全设置较大的缓冲区长度。
  默认值为102400，即如果每个消息体大小约为100个字节，则最大缓冲区大小将约为102400 \* 100B〜= 10MB。

### 使用

在[流定义](../../../guide/streams/overview.md#流属性)中指定自定义源， 相关属性为：

- TYPE：指定源名称，必须为驼峰式命名。
- CONF_KEY：指定要使用的配置键。

如果您开发了源实现 MySource，则应该具有：

1. 在插件文件中，将导出符号 MySource。
2. 编译的 MySource.so 文件位于 **plugins/sources** 内部。
3. 如果需要配置，请将 mySource.yaml 放在 **etc/sources** 中。

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
