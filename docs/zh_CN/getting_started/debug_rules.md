# 如何调试规则

eKuiper 是一个用于边缘计算的轻量级和高性能 SQL 引擎。它允许你编写类似 SQL
的规则来处理来自不同来源的流数据，并将结果发送到不同的外部系统中。规则创建很容易，但是，如果你的规则不能像预期那样工作呢？你如何找出问题所在并解决它？别担心，本文总结了一些最佳实践，帮助你可以像更容易地调试
eKuiper 规则。

## 创建规则

要调试一个规则，第一步就是要创建它。你可以通过使用 REST API 或 CLI来 做到这一点。在本教程中，我们将使用 REST API
进行所有的规则管理操作。下面是一个使用 REST API 创建规则的例子：

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT values.tag1 AS temperature, values.tag2 AS humidity FROM neuronStream",
  "actions": [
    {
      "influx": {
        "addr": "http://10.11.71.70:8086",
        "username": "",
        "password": "",
        "measurement": "test",
        "databasename": "mydb",
        "tagkey": "tagkey",
        "tagvalue": "tagvalue",
        "fields": "humidity,temperature"
      }
    }
  ]
}
```

### 调试方式

如果在创建规则时出现问题，你总是会得到一个错误信息。所以调试规则创建是非常直接的：只要检查你的请求是否返回错误，并检查错误的内容。

#### 检查你的 http 响应

在你用来发送http请求的工具中，你应该看到服务器的响应。如果规则创建成功，你会得到一个类似这样的响应：

```json
{
  "code": 200,
  "message": "OK"
}
```

如果创建有错误，错误信息可以在响应体中找到。例如，如果你试图用一个无效的 SQL 语句创建一个规则，你会得到一个类似这样的错误信息：

```json
{
  "code": 400,
  "message": "invalid sql: near \"SELEC\": syntax error"
}
```

#### 检查日志

通常情况下，通过响应体中的错误信息我们可以大致推断出错误的原因。但是如果你想知道更多关于错误的细节，可以检查 eKuiper 服务的日志。

日志位于 eKuiper 安装目录下的 `logs` 目录中。你可以使用 `tail` 命令来实时查看日志。

如果你使用Docker镜像，请确保通过环境变量`KUIPER__BASIC__CONSOLELOG=true`启用控制台日志，或者编辑`etc/kupier.yaml`
并将`consoleLog`设置为true。然后你可以使用`docker logs`命令来查看日志，请特别留意日志级别为错误的日志。

### 常见错误

当提交一个规则时，eKuiper 将验证该规则并运行。你可能会遇到一些错误，下面是一些常见的错误：

#### 语法错误

**1. SQL语法错误**

例如，提交一个带有 SQL `SELECT temperature humidity FROM sensor` 的规则时，你会得到这样的错误信息：

```text
HTTP/1.1 400 Bad Request

invalid rule json: Parse SQL SELECT temperature humidity FROM neuronStream error: found "humidity", expected FROM..
```

这个 SQL 语句中，两个字段之间缺少逗号，因此SQL解析器认为 `humidity` 是一个表名，并期望它的前面有 `FROM` 关键字。

要解决类似 "Parse SQL xxx error" 的错误时，只需仔细检查 SQL 语法并加以纠正。

**2. 未找到流定义**

在 eKuiper 中，创建规则前需要创建流。如果你试图使用一个不存在的流，你会得到一个类似这样的错误信息：

```text
HTTP/1.1 400 Bad Request

create rule topo error: fail to get stream myStream, please check if stream is created
```

为了解决这个错误，你需要先创建流。你可以使用 REST API 查看当前已定义的流，并在必要时创建一个新流。

**3. 规则已存在**

规则 ID 应当是唯一的。如果你试图用一个已经存在的 ID 创建一个规则，你会得到一个类似这样的错误信息：

```text
HTTP/1.1 400 Bad Request

store the rule error: Item rule1 already exists
```

要解决这个问题，你需要为你的规则使用一个不同的 ID，或者先删除现有的规则。

## 诊断规则

默认情况下，规则创建成功后将立即运行。如果你的规则期望将结果发送到一个 MQTT
主题，你可能已经订阅了那个主题并等待检查结果。但是，如果你没有收到任何消息时应该怎么办呢？接下来我们将讲解一些针对技巧，帮助用户找出问题所在。

### 调试方法

你可以按照以下步骤来诊断你的规则：

**1. 检查规则状态**

在创建规则时，我们只对语法做一些静态验证。当要运行规则时，有更多的事情需要考虑，比如外部数据源在运行时是否可用。所以，调试规则的第一步是检查规则的状态，看它是在正常运行还是由于一些运行时的错误而处于停止状态。

你可以使用REST API来检查规则的状态。例如，要检查规则`rule1`的状态，你可以发送这样的请求：

```http request
###
GET http://{{host}}/rules/rule1/status
```

如果规则没有正常运行，你会得到一个类似这样的响应：

```json
{
  "status": "stopped",
  "message": "Stopped: mqtt sink is missing property topic."
}
```

其中，`message` 字段告诉你该规则停止的可能原因。

**2. 检查指标**

如果规则运行良好，但你仍然没有得到你预期的结果，你可以检查指标，看看是否有任何问题。

使用上一节中的状态 API 来获取规则的度量指标。这些指标包括规则中从源、处理算子到 sink 的所有节点的信息。每个节点都有状态，如消息读入、写出、延迟等。

首先，看一下下面的源指标。如果你的源指标 `records_in_total`
是0，这意味着数据源没有收到任何数据。你需要检查数据源端：数据源是否已经发送了数据；你的源配置是否正确。例如，如果你的MQTT源主题配置为 `topic1`
，但你向`topic2` 发送数据，那么源将不会收到任何数据。

```
"source_demo_0_records_in_total": 0,
"source_demo_0_records_out_total": 0,
```

如果数据源的指标正常，接下来需要检查处理算子的指标，然后再检查 sink 的指标。例如，如果你的规则中有 `WHERE`
子句，则规则运行时将有一个 `Filter` 算子。该算子将在发送数据到 sink 之前过滤数据，因此你可能会发现 sink
中没有收到任何数据。你可以检查 `filter_xxx_records_in_total` 和 `filter_xxx_records_out_total` 指标。如果 `records_out`
和 `records_in` 不一样，则意味着一部分数据被过滤了。如果 `records_out` 是
0，则意味着所有数据都被过滤掉了。如果这不是预期行为，我们就需要进行进一步的调试。我们需要检查运行时真实的数据，确认是否满足过滤条件。这需要打开调试日志，检查或创建调试规则，并打印出数据。我们将在下一节介绍这个问题。

**3. 检查调试日志**

如果状态是停止的，你可以查看日志来检查细节。如果状态是运行的，但指标不符合预期，你可以检查日志，看看是否有任何错误，甚至打开调试来跟踪数据流。

下面是[检查日志的指令](#检查日志)。要打开调试日志，你可以在`etc/kuiper.yaml`文件中设置日志级别为`debug`
或者设置环境变量： `KUIPER__BASIC__DEBUG=false`。然后你就可以检查调试日志来查看数据流。例如，下面是关于过滤器的调试日志的一行。

```text
time="2023-05-31 14:58:43" level=debug msg="filter plan receive &{mockStream map[temperature:%!s(float64=-11.77) ts:%!s(float64=1.684738889251e+12)] %!s(int64=1685516298342) map[fi
le:C:\\repos\\go\\src\\github.com\\lfedge\\ekuiper\\data\\mock.lines] {{{%!s(int32=0) %!s(uint32=0)} %!s(uint32=0) %!s(uint32=0) {{} %!s(int32=0)} {{} %!s(int32=0)}} map[] map[]} {%!s(int32=0) %!s(uint32=0)} map[]}" file="operator/filter_operator.go:36" rule=rule1
```

该日志的行尾有 `rule=rule1`，这意味着这一行日志是由 rule1 打印的。在日志中，你可以发现 Filter
算子收到的数据是 `mockStream map[temperature:%！s(float64=-11.77) ts:%！s(float64=1.684738889251e+12)]`。这意味着流的名字是
mockStream，有效载荷是一个 `temperature=-11.77 and ts=1.684738889251e+12` 的键值对。我们可以根据这个实际数据检查 `WHERE`
条件，看看它是否正确匹配。

**4. 创建调试规则**

调试日志会把每条数据都打印出来，日志里通常很大，可能会让人不知所措。为了查看实际数据，我们也可以采用创建调试规则的方法，来辅助主规则的调试。主规则调试完成后再删除或关闭调试规则。例如，如果你在生产中的规则将数据发送到
MQTT，你可以添加一个 `log` 动作，将结果也打印在日志中。

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM mockStream WHERE temperature > 30",
  "actions": [
    {
      "mqtt": {
        "server": "{{broker address}}",
        "topic": "topic1"
      },
      "log": {
      }
    }
  ]
}
```

另一个例子是对 Filter 算子进行诊断。你可以创建另一条规则，打印出所有收到的数据，看看 Filter 算子是否按预期工作。

```json
{
  "id": "rule1_debug",
  "sql": "SELECT * FROM mockStream",
  "actions": [
    {
      "log": {
      }
    }
  ]
}
```

如果你的过滤算子使用计算过的数据作为条件，可以试着创建另一条规则来打印出所有相关的数据。例如，`SELECT * FROM mockStream WHERE temperature - lag(temperture) > 1`
。lag(temperature) 是一个由原始字段派生的数据。你可以创建一个调试规则来打印出 lag(temperature)，看看它是否符合预期。

## 端到端调试

在本节中，我们将编写一个简单的规则：从流中读取数据，并在温度上升超过1度时将其发送到 sink
中。我们将使用上述所有的调试技术来调试规则，最终确保该规则按预期工作。

首先，我们需要创建一个数据流。

```http request
###
POST http://{{host}}/streams
Content-Type: application/json

{"sql":"CREATE STREAM mockStream() WITH (DATASOURCE=\"data/mock\", FORMAT=\"json\", TYPE=\"mqtt\");"}
```

我们应该收到一个状态码为 200 的响应，并成功创建流。这个流是**无模式**的，它将订阅 MQTT 主题 `data/mock`
以接收数据。在实验中，我们假设数据是这样的：`{"temperature": 10, "humidity": 20}`。

### V1: 有语法错误的规则

以下是我们创建的规则的第一个版本，并通过 REST API 提交。

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT temperature, humidity FROM mockStream WHERE temprature - laig(temperature) > 1",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://yourserver:1883",
        "topic": "result"
      }
    }
  ]
}
```

我们会收到一个状态码为 400 的 HTTP 响应，错误消息如下所示：

```text
HTTP/1.1 400 Bad Request

Create rule error: Invalid rule json: Parse SQL SELECT temperature, humidity FROM mockStream WHERE temprature - laig(temperature) > 1 error: function laig not found.
```

错误信息清楚地告诉我们，我们使用了一个不存在的函数 `laig`。接下来，我们可以在规则中修复这个拼写错误。

### V2: 规则创建成功，但运行失败

解决了拼写错误之后，以下是我们第2个版本的规则。

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT temperature, humidity FROM mockStream WHERE temprature - lag(temperature) > 1",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://yourserver:1883",
        "topic": "result"
      }
    }
  ]
}
```

这一次，规则创建成功了，我们往前进了一步。然而，意外情况还是发生了，我们没有在结果主题上收到任何数据。让我们继续诊断。

首先，我们可以检查规则的状态。

```http request
###
GET http://{{host}}/rules/rule1/status
```

加入我们收到如下响应，则说明规则并未成功运行。

```json
{
  "status": "stopped",
  "message": "Stopped: found error when connecting for tcp://yourserver:1883: network Error : dial tcp: lookup yourserver: no such host."
}
```

看一下错误信息，原来是 MQTT 服务器配置错误。我们应该修复 sink 中的 MQTT 服务器配置，确保 MQTT 服务器已启动，然后再次启动规则。

```http request
###
POST http://{{host}}/rules/rule1/start
```

接着，查看规则状态。如果规则正常运行，我们可以收到如下回复：

```json
{
  "status": "running",
  "source_mockStream_0_records_in_total": 0,
  "source_mockStream_0_records_out_total": 0,
  "source_mockStream_0_process_latency_us": 0,
  "source_mockStream_0_buffer_length": 0,
  "source_mockStream_0_last_invocation": 0,
  "source_mockStream_0_exceptions_total": 0,
  "source_mockStream_0_last_exception": "",
  "source_mockStream_0_last_exception_time": 0,
  "op_2_analytic_0_records_in_total": 0,
  "op_2_analytic_0_records_out_total": 0,
  "op_2_analytic_0_process_latency_us": 0,
  "op_2_analytic_0_buffer_length": 0,
  "op_2_analytic_0_last_invocation": 0,
  "op_2_analytic_0_exceptions_total": 0,
  "op_2_analytic_0_last_exception": "",
  "op_2_analytic_0_last_exception_time": 0,
  "op_3_filter_0_records_in_total": 0,
  "op_3_filter_0_records_out_total": 0,
  "op_3_filter_0_process_latency_us": 0,
  "op_3_filter_0_buffer_length": 0,
  "op_3_filter_0_last_invocation": 0,
  "op_3_filter_0_exceptions_total": 0,
  "op_3_filter_0_last_exception": "",
  "op_3_filter_0_last_exception_time": 0,
  "op_4_project_0_records_in_total": 0,
  "op_4_project_0_records_out_total": 0,
  "op_4_project_0_process_latency_us": 0,
  "op_4_project_0_buffer_length": 0,
  "op_4_project_0_last_invocation": 0,
  "op_4_project_0_exceptions_total": 0,
  "op_4_project_0_last_exception": "",
  "op_4_project_0_last_exception_time": 0,
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0,
  "sink_mqtt_0_0_process_latency_us": 0,
  "sink_mqtt_0_0_buffer_length": 0,
  "sink_mqtt_0_0_last_invocation": 0,
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_last_exception": "",
  "sink_mqtt_0_0_last_exception_time": 0
}
```

规则终于正常运行了，只是还没收到数据，因为我们的数据源还没有数据。让我们往 `mockStream` 主题发送一些数据试试。

```json
{
  "temperature": 10,
  "humidity": 20
}
```

再次检查规则状态。指标没有变化，`source_mockStream_0_records_in_total`
仍然是0，这意味着规则还是没有收到数据。这很可能是数据源方面的问题。让我们再次检查我们的源配置，在这个例子中，检查流定义中
MQTT 服务器和主题配置。我们在流定义中把主题配置为`data/mock`，但我们却向`mockStream`主题发送数据，因此规则没有收到数据。

问题确认，接下来我们把数据发送到 `data/mock`
主题。再一次检查规则状态，这一次规则指标终于改变了。指标显示，我们收到了一条数据，在 `op_3_filter` 中被过滤，因此 MQTT
结果主题仍然没有收到数据。这是正常的，因为我们的过滤条件需要比较前后两条数据，当前只有一条数据。

```json
{
  "status": "running",
  "source_mockStream_0_records_in_total": 1,
  "source_mockStream_0_records_out_total": 1,
  "source_mockStream_0_process_latency_us": 753,
  "source_mockStream_0_buffer_length": 0,
  "source_mockStream_0_last_invocation": "2023-05-31T15:49:32.997547",
  "source_mockStream_0_exceptions_total": 0,
  "source_mockStream_0_last_exception": "",
  "source_mockStream_0_last_exception_time": 0,
  "op_2_analytic_0_records_in_total": 1,
  "op_2_analytic_0_records_out_total": 1,
  "op_2_analytic_0_process_latency_us": 0,
  "op_2_analytic_0_buffer_length": 0,
  "op_2_analytic_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_2_analytic_0_exceptions_total": 0,
  "op_2_analytic_0_last_exception": "",
  "op_2_analytic_0_last_exception_time": 0,
  "op_3_filter_0_records_in_total": 1,
  "op_3_filter_0_records_out_total": 0,
  "op_3_filter_0_process_latency_us": 0,
  "op_3_filter_0_buffer_length": 0,
  "op_3_filter_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_3_filter_0_exceptions_total": 0,
  "op_3_filter_0_last_exception": "",
  "op_3_filter_0_last_exception_time": 0,
  "op_4_project_0_records_in_total": 0,
  "op_4_project_0_records_out_total": 0,
  "op_4_project_0_process_latency_us": 0,
  "op_4_project_0_buffer_length": 0,
  "op_4_project_0_last_invocation": 0,
  "op_4_project_0_exceptions_total": 0,
  "op_4_project_0_last_exception": "",
  "op_4_project_0_last_exception_time": 0,
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0,
  "sink_mqtt_0_0_process_latency_us": 0,
  "sink_mqtt_0_0_buffer_length": 0,
  "sink_mqtt_0_0_last_invocation": 0,
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_last_exception": "",
  "sink_mqtt_0_0_last_exception_time": 0
}
```

### V3: 诊断过滤问题

让我们发送第二条数据到 `data/mock` 主题：

```json
{
  "temperature": 15,
  "humidity": 25
}
```

该数据中，温度值增加了 5，应该能够满足过滤条件。然而，我们还是没有收到结果数据。该如何诊断这种运行时问题呢？首先，我们还是先看一下规则指标：

```json
{
  "status": "running",
  "source_mockStream_0_records_in_total": 2,
  "source_mockStream_0_records_out_total": 2,
  "source_mockStream_0_process_latency_us": 753,
  "source_mockStream_0_buffer_length": 0,
  "source_mockStream_0_last_invocation": "2023-05-31T15:49:32.997547",
  "source_mockStream_0_exceptions_total": 0,
  "source_mockStream_0_last_exception": "",
  "source_mockStream_0_last_exception_time": 0,
  "op_2_analytic_0_records_in_total": 2,
  "op_2_analytic_0_records_out_total": 2,
  "op_2_analytic_0_process_latency_us": 0,
  "op_2_analytic_0_buffer_length": 0,
  "op_2_analytic_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_2_analytic_0_exceptions_total": 0,
  "op_2_analytic_0_last_exception": "",
  "op_2_analytic_0_last_exception_time": 0,
  "op_3_filter_0_records_in_total": 2,
  "op_3_filter_0_records_out_total": 0,
  "op_3_filter_0_process_latency_us": 0,
  "op_3_filter_0_buffer_length": 0,
  "op_3_filter_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_3_filter_0_exceptions_total": 0,
  "op_3_filter_0_last_exception": "",
  "op_3_filter_0_last_exception_time": 0,
  "op_4_project_0_records_in_total": 0,
  "op_4_project_0_records_out_total": 0,
  "op_4_project_0_process_latency_us": 0,
  "op_4_project_0_buffer_length": 0,
  "op_4_project_0_last_invocation": 0,
  "op_4_project_0_exceptions_total": 0,
  "op_4_project_0_last_exception": "",
  "op_4_project_0_last_exception_time": 0,
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0,
  "sink_mqtt_0_0_process_latency_us": 0,
  "sink_mqtt_0_0_buffer_length": 0,
  "sink_mqtt_0_0_last_invocation": 0,
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_last_exception": "",
  "sink_mqtt_0_0_last_exception_time": 0
}
```

从指标上看，我们知道数据被成功读取并流向过滤算子，但所有的数据都被过滤掉了。这是不符合预期的，接下来该如何诊断呢？我们可以启用调试日志来查看日志中的数据流，具体操作请阅读调试技巧部分的 "
3.检查调试日志"；或者也可以创建一个调试规则来了解过滤算子中的实际的计算数据。

在这个例子中，我们可以像下面这样创建一个调试规则：

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "ruleDebug",
  "sql": "SELECT temperature, humidity, temprature - lag(temperature) as diff FROM mockStream",
  "actions": [
    {
      "mqtt": {
        "server": "{{yourhost}}",
        "topic": "debug"
      }
    }
  ]
}
```

在这个调试规则中，我们移除了 `WHERE` 条件，并将条件中所用到的计算量 `temprature - lag(temperature)` 移动到了 `SELECT`
语句中，这样每条输入数据的运行时条件的计算量都会被打印出来。我们可以检查打印出来的值，看看为什么不满足条件。

接下来，重启两条规则，然后重新发送两条数据。我们可以看到，调试规则中打印出了两条数据：

```json lines
{
  "temperature": 10,
  "humidity": 20
}
{
  "temperature": 15,
  "humidity": 25
}
```

我们期望调试规则打印出 `diff` 的值，但结果中并没有，说明其计算结果为 `nil`
。这说明我们需要检查 `temprature - lag(temperature)` 这个条件。仔细检查一下，我们会发现我们有一个拼写错误 `temprature`,
实际应该是 `temperature`。这是一个常见的错误，当流是无模式的时候，这个错误是无法被 SQL 解析器发现。因为在无模式的情况下，SQL
解析器无法知道哪个字段是无效的。所以，在无模式的模式下，我们需要小心编写 SQL。

### V4: 最终可运行版本

让我们修复这个拼写错误，然后重新运行规则。

```http request
###
PUT http://{{host}}/rules/rule1
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT temperature, humidity FROM mockStream WHERE temperature - lag(temperature) > 1",
  "actions": [
    {
      "mqtt": {
        "server": "{{yourhost}}",
        "topic": "result"
      }
    }
  ]
}
```

规则重启后，指标会被重置。让我们从头开始发送数据到 `data/mock` 主题：

```json lines
{
  "temperature": 15,
  "humidity": 20
}
{
  "temperature": 20,
  "humidity": 25
}
```

我们终于在 `result` 主题中收到了数据：

```json
{
  "temperature": 20,
  "humidity": 25
}
```

至此，规则调试完成。我们可以继续发送更多测试数据，来验证规则的正确性。

## 总结

在本教程中，我们学习了如何通过指标、日志和调试规则来诊断一个规则。我们通过一个实例，一步步地完成了示例规则的创建和调试。希望本教程能帮助你诊断你的规则。