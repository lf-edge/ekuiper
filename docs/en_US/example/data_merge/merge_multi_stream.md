# Merge Data in Multiple Streams

## Problem

Due to security, cost, and other considerations, data often originates from different protocols or domains. Each protocol or domain may have its own data stream. For example, in the IIoT scenario, temperature and humidity sensor data may come from MQTT, while IT data may be provided by HTTP. The same situation arises in the IoV area. To extract meaningful insights, we need to merge data across streams. This article introduces how to merge data from multiple streams. Readers can customize calculations based on the examples in this article to meet their specific needs.

::: tip

To run the case by hand, please check [here](../howto.md).

:::

## Sample input

We mimic two streams of data, one for temperature and one for humidity. Thanks to the abstraction of eKuiper, the data source can be MQTT, HTTP, or any other protocol. The sample data is as follows:

**Data from stream1**

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

**Data from stream2**

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

## Desired output

Combine data from multiple streams for subsequent processing. A sample single event output is as follows:

```json
{
  "temperature": 27.23,
  "humidity": 79.66
}
```

Depending on the requirements of different scenarios, we can flexibly write rules to implement data merging, control how the data is merged, how often, and the output format of the merged data.

## Solution

In practice, users often have different merging algorithms. This article will list several common merge algorithms and how to use eKuiper SQL to implement them.

### 1. Output as One Stream by Rule Pipeline

In [Merge Multiple Devices' Data in a Single Stream](./merge_single_stream.md) tutorial,we introduced how to merge data in a single stream. When dealing with multiple streams, we can convert them into a single stream. The next steps remain the same as in the single stream case.

- Create rules for each stream to convert the data, and output to the same stream.
  - Rule 1 to sink stream to memory topic `merged`

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

  - Rule 2 to sink stream to memory topic `merged`

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

As shown in the above SQL, both rules sink the output to the same memory topic merged. In this example, we use the simplest select \* in the SQL to output all the data. In practice, users can perform calculations or filters according to actual needs to further filter the output.

- Create the memory stream `merged` to receive the union of the two rules.

  ```json
  {
    "sql": "CREATE STREAM mergedStream() WITH (TYPE=\"memory\",FORMAT=\"json\",DATASOURCE=\"merged\");"
  }
  ```

This stream is of `memory` type, and the data source is the memory topic `merged`, which is the output of the previous two streams. Thus, this new stream is the union of the two streams as one stream. The simplest rule select \* from mergedStream can output the merged data similarly like below:

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

Users can then use the solutions in [Merge Multiple Devices' Data in a Single Stream](./merge_single_stream.md) to merge the data.

### 2. Join Streams

If the data from different streams are related, we can use the join algorithm to merge the data. In stream processing systems, data is ingested as a sequence of unbounded events. However, the join operator requires a boundary for the data to be joined. Therefore, we need to add a window to collect a set of events for the join operation. The following is an example of joining two streams of data:

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

In this example, we use a 500 ms tumbling window to split the unbounded stream to a set of bounded windows. The join happens in each window. The join condition is that the difference between the timestamps of the data in two streams is less than 10 ms. The output sample is as below:

```json lines
[{"humidity":79.66,"temperature":27.23},{"humidity":83.86,"temperature":27.68},{"humidity":78.21,"temperature":27.06},{"humidity":75.4,"temperature":26.48}]
[{"humidity":80.85,"temperature":28.51},{"humidity":72.68,"temperature":31.57},{"humidity":76.34,"temperature":34.31},{"humidity":80.5,"temperature":30.34}]
```

Notice that, since window is used, the output frequency is now controlled by window and the output becomes a list. The equi-join is also widely used. `SELECT temperature, humidity FROM stream1 INNER JOIN stream2 ON stream1.device_id = stream2.device_id GROUP BY TumblingWindow(ms, 500)` is an example of equi-join if the data can be connected by device id.

### More merge algorithms

The above are some of the most common merge algorithms. If you have better merge algorithms and unique merge scenarios, please discuss in [GitHub Discussions](https://github.com/lf-edge/ekuiper/discussions/categories/use-case).
