# 降采样

降采样在数据源处对输入数据进行聚合操作，从而减少数据解码，变换等计算。降采样的功能本身可以通过时间窗口进行聚合计算的方式实现。然而时间窗口使用的是全采样数据源，丢弃的数据实际上也进行了解码等不必要的计算。相比时间窗口方案，数据源降采样主要是提升了数据处理的性能。采样频率与输入频率差别越大，性能和资源占用提升也越明显。数据源降采样适用于对性能要求更高的场景。

## 适用类型

数据流按照获取数据的方式可分为 PullSource 和 PushSource。其中，PullSource 为定期采样数据流，通过设置采样率可实现降采样的目的。而
PushSource 的数据流入频率由数据源的推送速度决定。在 eKuiper 中，提供通用的采样频率等配置，也可以实现各种策略的降采样。

## 配置

eKuiper 数据源提供了以下通用属性用于配置降采样特性。

### interval

采样间隔，格式为 go 的 duration 字符串，例如 "10s", "500ms" 等。
对于 PullSource，该属性决定了拉数据的间隔。
对于 PushSource，数据源推送的数据会累计，直到采样间隔再通过配置的采样策略发出。

### mergeField

默认的降采样策略取采样周期内最后的消息。若配置 mergeField，则会取每个 mergeField 对应的最后一条消息，然后合并成一条消息输出。

mergeField 配置为消息中的列名，例如 id。则不同 id 的数据会聚合为一条降采样数据。
注意：

- 只有支持按列解码的格式支持配置此属性。当前，仅有 JSON 格式支持。用户自定义的格式可通过实现 `message.PartialDecoder`
  实现按列解码。
- mergeField 当前仅支持顶层非复合类型的字段。嵌套字段或者数组/结构体等复合类型将作为相同 key 聚合。

## 降采样策略案例

Source 降采样输入都是采样周期内的 N 条消息，输出为单条消息。如何从 N 条消息转为单条消息是由降采样策略决定的。目前我们支持取最新值以及按列聚合两种降采样策略。

以 MQTT 数据源为例，通过订阅 MQTT 主题，数据源会收到推送的数据，例如 1Hz 或者更高频率的数据。假设计算中不需要如此高频的数据，我们可以配置
1s 降采样，减少计算的资源消耗。以下为两种降采样策略的配置案例。

### 取最新值

该策略在采样周期内收到 N 条数据，会输出最后一条数据。

1. 创建降采样配置：通过以下 REST API, 我们创建了 MQTT 配置 `onesec`。其中包含了配置项 `interval`, 配置采样周期为 1
   秒。注意：MQTT 服务器地址等配置，将会沿用默认配置。

   ```http request
   ###
   PUT http://{{host}}/metadata/sources/mqtt/confKeys/onesec

   {
     "interval": "1s"
   }

   ```

2. 创建数据流：以下 API 创建了名为 mqttOneSec 数据流，通过 `CONF_KEY="onesec"` 采用了第一步创建的降采样配置。

   ```http request
   ###
   POST http://{{host}}/streams
   Content-Type: application/json

   {
     "sql": "CREATE STREAM mqttOneSec() WITH (TYPE=\"mqtt\",FORMAT=\"json\",DATASOURCE=\"demo\",CONF_KEY=\"onesec\");"
   }
   ```

3. 基于降采样数据流创建规则：接下来用户可基于该数据流创建规则。以下为最简单的规则，取出所有数据发送到 MQTT 。该规则将收到 1s
   采样率的数据，即每一秒的最后一条数据。

   ```http request
   ###
   POST http://{{host}}/rules
   Content-Type: application/json

   {
     "id": "ruleOneSecLatest",
     "sql": "SELECT * FROM mqttOneSec",
     "actions": [
       {
         "mqtt": {
           "server": "tcp://127.0.0.1:1883",
           "topic": "result/onesec",
           "sendSingle": true
         }
       }
     ]
   }
   ```

### 按列聚合

该策略在采样周期内收到 N 条数据，会将这些数据按指定的列的值区分，合并每种列的最后一条数据为一条完整的数据。该策略适合数据流本身包含不同来源不同
schema 数据的情况。例如，输入可能是

```json lines
{"id":1, "temperature":20}
{"id":2, "humidity":80}
{"id":1, "temperature":30}
```

聚合后结果为单条数据：

```json
{ "id": 1, "temperature": 30, "humidity": 80 }
```

1. 创建降采样配置：通过以下 REST API, 我们创建了 MQTT 配置 `onesec_merge`。其中包含了配置项 `interval`, 配置采样周期为 1
   秒。也配置了聚合列 `mergeField`。注意：MQTT 服务器地址等配置，将会沿用默认配置。

   ```http request
   ###
   PUT http://{{host}}/metadata/sources/mqtt/confKeys/onesec_merge

   {
     "interval": "1s",
     "mergeField": "id"
   }
   ```

2. 创建数据流：以下 API 创建了名为 mqttOneSecM 数据流，通过 `CONF_KEY="onesec_merge"` 采用了第一步创建的降采样配置。

   ```http request
    ###
    POST http://{{host}}/streams
    Content-Type: application/json

    {
      "sql": "CREATE STREAM mqttOneSecM() WITH (TYPE=\"mqtt\",FORMAT=\"json\",DATASOURCE=\"demo\",CONF_KEY=\"onesec_merge\");"
    }
   ```

3. 基于降采样数据流创建规则：接下来用户可基于该数据流创建规则。以下为最简单的规则，取出所有数据发送到 MQTT 。该规则将收到 1s
   采样率的数据，按列聚合为单条数据后发送。

   ```http request
    ###
    POST http://{{host}}/rules
    Content-Type: application/json

    {
      "id": "RuleOneSecM",
      "sql": "SELECT * FROM mqttOneSecM",
      "actions": [
        {
          "mqtt": {
            "server": "tcp://127.0.0.1:1883",
            "topic": "result/onesecm",
            "sendSingle": true
          }
        }
      ]
    }
   ```

### 全聚合？

按列聚合规则需要指定聚合列，如果不指定列做聚合应该如何做呢？建议采用时间窗口的方式，通过 merge_agg 函数进行聚合。

```SQL
SELECT merge_agg(*) FROM normalStream GROUP BY TumblingWindow(ss, 1)
```

为何这样设计：降采样数据流的初衷是减少不必要的计算，从而降低 CPU
和资源占用。目前两种降采样策略都只需要很少的解码计算量即可判断数据是否需要采样计算。而全聚合需要对每条数据进行消耗大量资源的解码操作，本质上数据源测并没有降采样，而是在全采样之后再做数据变换。因此，用户可直接采用全采样数据源，通过窗口函数进行聚合运算。

## 降采样指标观测

数据源降采样在 ratelimit 算子中实现。获取规则运行指标，通过其中 ratelimit 算子的运行指标可观测降采样的运行情况。如下例子中：
`source_mqttOneMiMerge_0_records_out_total` 可知 MQTT 数据源读入 25 条数据。
ratelimit 算子的指标包括 `op_2_ratelimit_0_records_in_total`: 25 和 `op_2_ratelimit_0_records_out_total`: 1 表示读入 25
条数据，并降采样为 1 条。之后的 decode 算子仅需解析降采样后的数据。

```json
{
  "status": "running",
  "lastStartTimestamp": "1720151899579",
  "lastStopTimestamp": "0",
  "nextStopTimestamp": "0",
  "source_mqttOneMiMerge_0_records_in_total": 25,
  "source_mqttOneMiMerge_0_records_out_total": 25,
  "source_mqttOneMiMerge_0_messages_processed_total": 25,
  "source_mqttOneMiMerge_0_process_latency_us": 0,
  "source_mqttOneMiMerge_0_buffer_length": 0,
  "source_mqttOneMiMerge_0_last_invocation": "2024-07-05T11:58:40.733398",
  "source_mqttOneMiMerge_0_exceptions_total": 0,
  "source_mqttOneMiMerge_0_last_exception": "",
  "source_mqttOneMiMerge_0_last_exception_time": 0,
  "op_2_ratelimit_0_records_in_total": 25,
  "op_2_ratelimit_0_records_out_total": 1,
  "op_2_ratelimit_0_messages_processed_total": 25,
  "op_2_ratelimit_0_process_latency_us": 0,
  "op_2_ratelimit_0_buffer_length": 0,
  "op_2_ratelimit_0_last_invocation": "2024-07-05T11:58:40.733398",
  "op_2_ratelimit_0_exceptions_total": 0,
  "op_2_ratelimit_0_last_exception": "",
  "op_2_ratelimit_0_last_exception_time": 0,
  "op_3_payload_decoder_0_records_in_total": 1,
  "op_3_payload_decoder_0_records_out_total": 1,
  "op_3_payload_decoder_0_messages_processed_total": 1,
  "op_3_payload_decoder_0_process_latency_us": 0,
  "op_3_payload_decoder_0_buffer_length": 0,
  "op_3_payload_decoder_0_last_invocation": "2024-07-05T11:59:19.59698",
  "op_3_payload_decoder_0_exceptions_total": 0,
  "op_3_payload_decoder_0_last_exception": "",
  "op_3_payload_decoder_0_last_exception_time": 0,
  "op_4_project_0_records_in_total": 1,
  "op_4_project_0_records_out_total": 1,
  "op_4_project_0_messages_processed_total": 1,
  "op_4_project_0_process_latency_us": 0,
  "op_4_project_0_buffer_length": 0,
  "op_4_project_0_last_invocation": "2024-07-05T11:59:19.59698",
  "op_4_project_0_exceptions_total": 0,
  "op_4_project_0_last_exception": "",
  "op_4_project_0_last_exception_time": 0,
  "op_mqtt_0_0_transform_0_records_in_total": 1,
  "op_mqtt_0_0_transform_0_records_out_total": 1,
  "op_mqtt_0_0_transform_0_messages_processed_total": 1,
  "op_mqtt_0_0_transform_0_process_latency_us": 0,
  "op_mqtt_0_0_transform_0_buffer_length": 0,
  "op_mqtt_0_0_transform_0_last_invocation": "2024-07-05T11:59:19.59698",
  "op_mqtt_0_0_transform_0_exceptions_total": 0,
  "op_mqtt_0_0_transform_0_last_exception": "",
  "op_mqtt_0_0_transform_0_last_exception_time": 0,
  "op_mqtt_0_1_encode_0_records_in_total": 1,
  "op_mqtt_0_1_encode_0_records_out_total": 1,
  "op_mqtt_0_1_encode_0_messages_processed_total": 1,
  "op_mqtt_0_1_encode_0_process_latency_us": 0,
  "op_mqtt_0_1_encode_0_buffer_length": 0,
  "op_mqtt_0_1_encode_0_last_invocation": "2024-07-05T11:59:19.59698",
  "op_mqtt_0_1_encode_0_exceptions_total": 0,
  "op_mqtt_0_1_encode_0_last_exception": "",
  "op_mqtt_0_1_encode_0_last_exception_time": 0,
  "sink_mqtt_0_0_records_in_total": 1,
  "sink_mqtt_0_0_records_out_total": 1,
  "sink_mqtt_0_0_messages_processed_total": 1,
  "sink_mqtt_0_0_process_latency_us": 0,
  "sink_mqtt_0_0_buffer_length": 0,
  "sink_mqtt_0_0_last_invocation": "2024-07-05T11:59:19.59698",
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_last_exception": "",
  "sink_mqtt_0_0_last_exception_time": 0
}
```
