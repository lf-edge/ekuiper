# 合并多个数据流

## 问题

由于安全、成本和其他考虑因素，数据通常来自不同的协议或领域。每个协议或领域可能有自己的数据流。例如，在工业物联网（IIoT）场景中，温度和湿度传感器的数据可能来自MQTT，而IT数据可能由HTTP提供。在物联网汽车（IoV）领域也存在类似情况。为了提取有意义的洞察，我们需要跨流合并数据。本文介绍了如何从多个数据流中合并数据。读者还可以根据本文中的示例添加自定义计算，以满足其特定需求。

::: tip

运行案例，请查看[这里](../howto.md).

:::

## 示例输入

我们模拟两个数据流，一个用于温度，另一个用于湿度。由于eKuiper的抽象，数据源可以是MQTT、HTTP或任何其他协议。示例数据如下：

**stream1 数据**

```json lines
{"device_id":"A","temperature":27.23,"ts":1681786070368}
{"device_id":"A","temperature":27.68,"ts":1681786070479}
{"device_id":"A","temperature":27.28,"ts":1681786070588}
{"device_id":"A","temperature":27.06,"ts":1681786070700}
{"device_id":"A","temperature":26.48,"ts":1681786070810}
{"device_id":"A","temperature":28.51,"ts":1681786070921}
{"device_id":"A","temperature":31.57,"ts":1681786071031}
{"device_id":"A","temperature":31.87,"ts":1681786071140}
{"device_id":"A","temperature":34.31,"ts":1681786071252}
{"device_id":"A","temperature":30.34,"ts":1681786071362}
```

**stream2 数据**

```json lines
{"device_id":"B","humidity":79.66,"ts":1681786070367}
{"device_id":"B","humidity":83.86,"ts":1681786070477}
{"device_id":"B","humidity":75.79,"ts":1681786070590}
{"device_id":"B","humidity":78.21,"ts":1681786070698}
{"device_id":"B","humidity":75.4,"ts":1681786070808}
{"device_id":"B","humidity":80.85,"ts":1681786070919}
{"device_id":"B","humidity":72.68,"ts":1681786071029}
{"device_id":"B","humidity":73.86,"ts":1681786071142}
{"device_id":"B","humidity":76.34,"ts":1681786071250}
{"device_id":"B","humidity":80.5,"ts":1681786071361}
```

## 期望输出

将来自不同数据流的数据合并以供后续处理。单个事件输出示例如下：

```json
{
  "temperature": 27.23,
  "humidity": 79.66
}
```

根据不同场景的要求，我们可以灵活编写规则来实现数据合并，控制数据的合并方式、频率以及合并数据的输出格式。

## 解决方案

在实践中，用户通常使用不同的合并算法。本文将列出几种常见的合并算法以及如何使用eKuiper SQL来实现它们。

### 1. 通过规则流水线将多流合并为一个流

在[合并单流多设备数据](./merge_single_stream.md)教程中，我们介绍了如何在单个流中合并数据。在处理多个流时，我们可以将多个流转换为单个流。下一步与单个流的情况相同。

- 为每个流创建规则以转换数据，并将其输出到相同的流中。
  - Rule1 将 stream1 的数据汇入内存主题 `merged`

  ```json
  {
    "id": "ruleMerge1",
    "name": "Rule to send data from stream1 to merged stream",
    "sql": "SELECT * FROM stream1",
    "actions": [
      {
        "memory": {
          "topic": "merged",
          "sendSingle": true
        }
      }
    ]
  }
  ```

  - Rule2 将 stream2 的数据汇入内存主题 `merged`

  ```json
  {
    "id": "ruleMerge2",
    "name": "Rule to send data from stream2 to merged stream",
    "sql": "SELECT * FROM stream2",
    "actions": [
      {
        "memory": {
          "topic": "merged",
          "sendSingle": true
        }
      }
    ]
  }
  ```

如上所示，两个规则都将输出发送到相同的内存主题`merged`。在此示例中，我们在SQL中使用最简单的`select *` 以输出所有数据。在实践中，用户可以根据实际需求执行计算或过滤，以进一步过滤输出。

- 创建内存流 `merged` 以接收两个规则输出的并集。

  ```json
  {
    "sql": "CREATE STREAM mergedStream() WITH (TYPE=\"memory\",FORMAT=\"json\",DATASOURCE=\"merged\");"
  }
  ```

此流为 `memory` 类型，数据源是内存主题 `merged`，这是前两个流的输出。因此，这个新流是两个流的并集作为一个流。最简单的规则 `select * from mergedStream` 可以输出合并的数据，类似于下面的例子：

```text
{"device_id":"B","humidity":79.66,"ts":1681786070367}
{"device_id":"A","temperature":27.23,"ts":1681786070368}
{"device_id":"B","humidity":83.86,"ts":1681786070477}
{"device_id":"A","temperature":27.68,"ts":1681786070479}
{"device_id":"A","temperature":27.28,"ts":1681786070588}
{"device_id":"B","humidity":75.79,"ts":1681786070590}
{"device_id":"B","humidity":78.21,"ts":1681786070698}
{"device_id":"A","temperature":27.06,"ts":1681786070700}
```

然后，用户可以使用[合并单流多设备数据](./merge_single_stream.md)中的解决方案合并数据。

### 2. 连接流

如果来自不同流的数据是相关的，则可以使用连接算子合并数据。在流处理系统中，数据是无界的一系列事件。然而，连接运算符需要用于连接的数据的边界。因此，我们需要添加窗口来收集用于连接操作的事件集。以下是连接两个数据流的示例：

```json
{
  "id": "ruleJoin",
  "name": "Rule to join data from stream1 and stream2",
  "sql": "SELECT temperature, humidity FROM stream1 INNER JOIN stream2 ON stream1.ts - stream2.ts BETWEEN 0 AND 10 GROUP BY TumblingWindow(ms, 500)",
  "actions": [
    {
      "log": {}
    }
  ]
}
```

在此示例中，我们使用500毫秒的滚动窗口将无界流拆分为一组有界窗口。连接发生在每个窗口中。连接条件是两个流中数据的时间戳之间的差异小于10毫秒。输出样本如下：

```json lines
[{"humidity":79.66,"temperature":27.23},{"humidity":83.86,"temperature":27.68},{"humidity":78.21,"temperature":27.06},{"humidity":75.4,"temperature":26.48}]
[{"humidity":80.85,"temperature":28.51},{"humidity":72.68,"temperature":31.57},{"humidity":76.34,"temperature":34.31},{"humidity":80.5,"temperature":30.34}]
```

请注意，由于使用了窗口，输出频率由窗口频率决定，输出的内容变为列表。等值连接也是广泛使用的。如果数据可以通过设备 ID 连接，那么可使用 `SELECT temperature, humidity FROM stream1 INNER JOIN stream2 ON stream1.device_id = stream2.device_id GROUP BY TumblingWindow(ms, 500)` 进行等着连接。

### 更多合并算法

上面是一些常见的合并算法。如果您有更好的合并算法和独特的合并场景，欢迎在 [GitHub Discussions](https://github.com/lf-edge/ekuiper/discussions/categories/use-case) 中与我们分享。
