# Down Sampling

The process of down sampling aggregates the input data at the source, thereby reducing the computation required for data
decoding and transformation. The functionality of down sampling itself can be achieved through the aggregation
calculation using a time window. However, the time window uses the full sampled data source, and the discarded data has
actually undergone unnecessary computations such as decoding. Compared to the time window scheme, down sampling at the
data source mainly enhances the performance of data processing. The greater the difference between the sampling
frequency and the input frequency, the more significant the performance and resource utilization improvement. Data
source down sampling is suitable for scenarios with higher performance requirements.

## Applicable Types

Data streams can be categorized into PullSource and PushSource based on the method of data acquisition. PullSource is a
data stream that samples data periodically, and down sampling can be achieved by setting the sampling rate. The data
inflow frequency of PushSource is determined by the push speed of the data source. In eKuiper, common configurations
such as sampling frequency are provided, and various strategies for down sampling can also be implemented.

## Configuration

eKuiper data sources provide the following common properties for configuring down sampling characteristics.

### interval

The sampling interval, in the format of a Go duration string, such as "10s", "500ms", etc.
For PullSource, this property determines the interval at which data is pulled.
For PushSource, the data pushed by the data source will accumulate until the sampling interval, and then it will be
emitted according to the configured sampling strategy.

### mergeField

The default down sampling strategy takes the last message within the sampling period. If mergeField is configured, it
will take the last message corresponding to each mergeField and then merge them into a single message for output.

The mergeField is configured as the column name in the message, such as "id". Data with different ids will be aggregated
into a single downs ample data.

Notes:

- Only formats that support columnar decoding support this property. Currently, only the JSON format is supported. Users
  can implement `message.PartialDecoder` to support columnar decoding for custom formats.
- The mergeField currently only supports top-level non-composite fields. Nested fields or composite types such as arrays
  or structures will be aggregated as the same key.

## Down sampling Strategy Examples

The input for Source down sampling is N messages within the sampling period, and the output is a single message. How to
convert from N messages to a single message is determined by the down sampling strategy. Currently, we support two down
sampling strategies: taking the latest value and aggregating by column.

Taking the MQTT data source as an example, by subscribing to the MQTT topic, the data source will receive pushed data,
such as 1Hz or higher frequency data. Suppose the calculation does not require such high-frequency data; we can
configure a 1-second down sampling to reduce the consumption of computing resources. The following are examples of
configurations for the two down sampling strategies.

### Taking the Latest Value

This strategy will output the last data received within the sampling period when it receives N data.

1. Create down sampling configuration: Through the following REST API, we create the MQTT configuration `onesec`. It
   includes the configuration item `interval`, setting the sampling period to 1 second. Note: The MQTT server address
   and other configurations will follow the default settings.

   ```http request
   PUT http://{{host}}/metadata/sources/mqtt/confKeys/onesec

   {
     "interval": "1s"
   }
   ```

2. Create data stream: The following API creates a data stream named `mqttOneSec`, which uses the down sampling
   configuration created in step 1 through `CONF_KEY="onesec"`.

   ```http request
   POST http://{{host}}/streams
   Content-Type: application/json

   {
     "sql": "CREATE STREAM mqttOneSec() WITH (TYPE=\"mqtt\",FORMAT=\"json\",DATASOURCE=\"demo\",CONF_KEY=\"onesec\");"
   }
   ```

3. Create rules based on the down sampling data stream: Next, users can create rules based on this data stream. The
   following is the simplest rule, which sends all data to MQTT. This rule will receive data at a 1-second sampling
   rate, that is, the last data of each second.

   ```http request
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

### Column Aggregation

This strategy, when it receives N data entries within the sampling period, will distinguish these entries by the values
of the specified columns and merge the last entry for each column into a complete data entry. This strategy is suitable
for situations where the data stream inherently contains data from different sources with different schemas. For
example, the input might be:

```json lines
{
  "id": 1,
  "temperature": 20
}
{
  "id": 2,
  "humidity": 80
}
{
  "id": 1,
  "temperature": 30
}
```

The aggregated result is a single data entry:

```json
{
  "id": 1,
  "temperature": 30,
  "humidity": 80
}
```

1. Create downsampling configuration: Through the following REST API, we create the MQTT configuration `onesec_merge`.
   It includes the `interval` configuration item, setting the sampling period to 1 second. It also configures
   the `mergeField` for aggregation. Note: The MQTT server address and other configurations will follow the default
   settings.

   ```http request
   PUT http://{{host}}/metadata/sources/mqtt/confKeys/onesec_merge

   {
     "interval": "1s",
     "mergeField": "id"
   }
   ```

2. Create data stream: The following API creates a data stream named `mqttOneSecM`, which adopts the downsampling
   configuration created in the first step through `CONF_KEY="onesec_merge"`.

   ```http request
   POST http://{{host}}/streams
   Content-Type: application/json

   {
     "sql": "CREATE STREAM mqttOneSecM() WITH (TYPE=\"mqtt\",FORMAT=\"json\",DATASOURCE=\"demo\",CONF_KEY=\"onesec_merge\");"
   }
   ```

3. Create rules based on the downsampling data stream: Next, users can create rules based on this data stream. The
   following is the simplest rule, which takes all data and sends it to MQTT. This rule will receive data at a 1-second
   sampling rate, aggregate it into a single data entry by column, and then send it.

   ```http request
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

### Full Aggregation?

The column-based aggregation rule requires a specified column for aggregation. What if you want to aggregate without
specifying a column? It is recommended to use a time window approach and perform aggregation through the `merge_agg`
function.

```SQL
SELECT merge_agg(*)
FROM normalStream
GROUP BY TumblingWindow(ss, 1)
```

Why this design: The original intention of the down sampling data stream is to reduce unnecessary computations, thereby
decreasing CPU and resource usage. The current two down sampling strategies only require minimal decoding computations
to determine whether the data needs to be sampled and computed. Full aggregation, however, requires a resource-intensive
decoding operation for each data entry. Essentially, there is no down sampling at the data source level; instead, data
transformation is performed after full sampling. Therefore, users can directly use a fully sampled data source and
perform aggregation operations through window functions.

## Down sampling Metrics Observation

Data source downsampling is implemented in the `ratelimit` operator. To obtain rule execution metrics, you can observe
the operation of downsampling through the metrics of the `ratelimit` operator. For example, in the following case:
`source_mqttOneMiMerge_0_records_out_total` indicates that the MQTT data source has read in 25 data entries.
The metrics of the `ratelimit` operator include `op_2_ratelimit_0_records_in_total`: 25
and `op_2_ratelimit_0_records_out_total`: 1, which means that 25 data entries were read in and downsample to 1 entry.
The subsequent `decode` operator only needs to parse the downsampled data.

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
