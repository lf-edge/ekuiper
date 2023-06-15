# Merge Multiple Devices' Data in Single Stream

To run the case by hand, please check [here](../howto.md).

## Problem

In IoT scenarios, devices such as sensors are often numerous, and usually the acquisition software combines data from all devices into one data stream. Since each sensor has different acquisition and response cycles, the data stream is interspersed with data from various devices, and the data is more fragmented, with each event containing data from only one sensor. For example, if sensor A collects temperature data once per second, sensor B collects humidity data every 5 seconds, and sensor C collects data every 10 seconds, then there will be three kinds of data in the data stream, A, B, and C. Each kind of data is collected at different frequencies, but they are all mixed together. Back-end applications where the settings of the same group of sensors are usually correlated need to merge data from the same group of sensors together for subsequent processing.

## Sample input

The temperature and humidity sensor data is mixed in the data stream, and none of the data is complete.

```text
{"device_id":"B","humidity":79.66,"ts":1681786070367}
{"device_id":"A","temperature":27.23,"ts":1681786070368}
{"device_id":"B","humidity":83.86,"ts":1681786070477}
{"device_id":"A","temperature":27.68,"ts":1681786070479}
{"device_id":"A","temperature":27.28,"ts":1681786070588}
{"device_id":"B","humidity":75.79,"ts":1681786070590}
{"device_id":"B","humidity":78.21,"ts":1681786070698}
{"device_id":"A","temperature":27.06,"ts":1681786070700}
{"device_id":"B","humidity":75.4,"ts":1681786070808}
{"device_id":"A","temperature":26.48,"ts":1681786070810}
{"device_id":"B","humidity":80.85,"ts":1681786070919}
{"device_id":"A","temperature":28.51,"ts":1681786070921}
{"device_id":"B","humidity":72.68,"ts":1681786071029}
{"device_id":"A","temperature":31.57,"ts":1681786071031}
{"device_id":"A","temperature":31.87,"ts":1681786071140}
{"device_id":"B","humidity":73.86,"ts":1681786071142}
{"device_id":"B","humidity":76.34,"ts":1681786071250}
{"device_id":"A","temperature":34.31,"ts":1681786071252}
{"device_id":"B","humidity":80.5,"ts":1681786071361}
{"device_id":"A","temperature":30.34,"ts":1681786071362}
```

## Desired output

Combine data from the same group of sensors (temperature and humidity) for subsequent processing. A sample single event output is as follows:

```json
{
  "temperature": 27.23,
  "humidity": 79.66,
  "ts": 1681786070368
}
```

Depending on the requirements of different scenarios, we can flexibly write rules to implement data merging, control how the data is merged, how often, and the output format of the merged data.

## Solution

In practice, users often have different merging algorithms. This article will list several common merge algorithms and how to use eKuiper SQL to implement them.

### 1. Output once per event

This merge algorithm is the simplest. Each time an event arrives, the latest values of temperature and humidity are obtained, combined and sent out. This algorithm outputs data at the same frequency as the input.

```SQL
SELECT latest(temperature, 0) as temperature, latest(humidity, 0) as humidity, ts FROM demoStream
```

As shown in the above SQL, latest(temperature, 0) will get the latest temperature value. That is, if there is a temperature value in the current event, return that value; otherwise, return the last temperature value received before. If there is no temperature value before, return 0. The humidity data is the same. In this way, whenever a single temperature or single humidity event is received, it will be combined into an event containing temperature and humidity and sent out.

With this rule, from the sample input sequence we can get the following output:

```text
{"humidity":79.66,"temperature":0,"ts":1681786070367}
{"humidity":79.66,"temperature":27.23,"ts":1681786070368}
{"humidity":83.86,"temperature":27.23,"ts":1681786070477}
{"humidity":83.86,"temperature":27.68,"ts":1681786070479}
{"humidity":83.86,"temperature":27.28,"ts":1681786070588}
{"humidity":75.79,"temperature":27.28,"ts":1681786070590}
{"humidity":78.21,"temperature":27.28,"ts":1681786070698}
{"humidity":78.21,"temperature":27.06,"ts":1681786070700}
{"humidity":75.4,"temperature":27.06,"ts":1681786070808}
{"humidity":75.4,"temperature":26.48,"ts":1681786070810}
{"humidity":80.85,"temperature":26.48,"ts":1681786070919}
{"humidity":80.85,"temperature":28.51,"ts":1681786070921}
{"humidity":72.68,"temperature":28.51,"ts":1681786071029}
{"humidity":72.68,"temperature":31.57,"ts":1681786071031}
{"humidity":72.68,"temperature":31.87,"ts":1681786071140}
{"humidity":73.86,"temperature":31.87,"ts":1681786071142}
{"humidity":76.34,"temperature":31.87,"ts":1681786071250}
{"humidity":76.34,"temperature":34.31,"ts":1681786071252}
{"humidity":80.5,"temperature":34.31,"ts":1681786071361}
{"humidity":80.5,"temperature":30.34,"ts":1681786071362}
```

Users can add `where` statements according to actual needs to further filter the output, such as filtering by timestamp in [solution 3](#3-merge-data-with-close-time).

### 2. Output based on temperature

This merge algorithm is based on temperature as the main index. Each time a temperature event is received, the latest temperature and humidity values are obtained and combined to send out. The output frequency of this algorithm is the same as the input frequency of the main index temperature, and the humidity is used as an auxiliary index to complete the data.

```SQL
SELECT temperature, latest(humidity, 0) as humidity, ts FROM demoStream WHERE isNull(temperature) = false
```

As shown in the above SQL, `WHERE isNull(temperature) = false` will filter out events that do not contain temperature values. In this way, whenever a temperature event is received, it will be combined into an event containing temperature and humidity and sent out.

With this rule, from the sample input sequence we can get the following output:

```text
{"humidity":79.66,"temperature":27.23,"ts":1681786070368}
{"humidity":83.86,"temperature":27.68,"ts":1681786070479}
{"humidity":83.86,"temperature":27.28,"ts":1681786070588}
{"humidity":78.21,"temperature":27.06,"ts":1681786070700}
{"humidity":75.4,"temperature":26.48,"ts":1681786070810}
{"humidity":80.85,"temperature":28.51,"ts":1681786070921}
{"humidity":72.68,"temperature":31.57,"ts":1681786071031}
{"humidity":72.68,"temperature":31.87,"ts":1681786071140}
{"humidity":76.34,"temperature":34.31,"ts":1681786071252}
{"humidity":80.5,"temperature":30.34,"ts":1681786071362}
```

### 3. Merge data with close time

This merge algorithm is based on the assumption that the data collection frequency of each sensor is the same, and the data received at the same time should contain all the sensor data needed. However, the time when each data is received is not fixed. Take temperature and humidity as an example, the rule may receive temperature data first, or humidity data first, but the time interval between the same batch of data should be close; conversely, the time interval between different batches of data is relatively large.

```SQL
SELECT latest(temperature, 0) as temperature, latest(humidity, 0) as humidity, ts FROM demoStream WHERE ts - lag(ts) < 10
```

As shown in the above SQL, `WHERE ts - lag(ts) < 10` will filter out events with a time interval greater than 10 milliseconds from the previous event. In this way, whenever a temperature or humidity event is received, only the second event that satisfies the condition that the time interval between the current event and the previous event is less than 10 milliseconds will be combined into an event containing temperature and humidity and sent out.

With this rule, from the sample input sequence we can get the following output:

```text
{"humidity":79.66,"temperature":27.23,"ts":1681786070368}
{"humidity":83.86,"temperature":27.68,"ts":1681786070479}
{"humidity":75.79,"temperature":27.28,"ts":1681786070590}
{"humidity":78.21,"temperature":27.06,"ts":1681786070700}
{"humidity":75.4,"temperature":26.48,"ts":1681786070810}
{"humidity":80.85,"temperature":28.51,"ts":1681786070921}
{"humidity":72.68,"temperature":31.57,"ts":1681786071031}
{"humidity":73.86,"temperature":31.87,"ts":1681786071142}
{"humidity":76.34,"temperature":34.31,"ts":1681786071252}
{"humidity":80.5,"temperature":30.34,"ts":1681786071362}
```

### 4. Fixed interval average output

Previous algorithms are all based on collecting all data as the goal, but in actual application, users may not be interested in each individual real-time value, but in the trend of a certain index such as the average value. In this case, we can use the `TUMBLINGWINDOW` time window, and the data in each time window will be merged into one piece of data and aggregated. Since our sample data is relatively short, there is only 1 second of data in total, in order to get the output, we set the time window here to be relatively short 500 milliseconds. In order to get a fixed result, we use event time to calculate the window, so that each window can be calculated at a fixed time.

```SQL
SELECT avg(temperature) as temperature, avg(humidity) as humidity, window_end() as ts FROM demoStream GROUP BY TUMBLINGWINDOW(ms, 500)
```

As shown in the above SQL, `GROUP BY TUMBLINGWINDOW(ms, 500)` will merge each 500 milliseconds of data into one piece of data, and then calculate the average value of temperature and humidity separately. In this way, every 500 milliseconds, we can get an event containing temperature and humidity.

With this rule, from the sample input sequence we can get the following output:

```text
{"humidity":81.75999999999999,"temperature":27.455,"ts":1681786070500}
{"humidity":77.5625,"temperature":27.332500000000003,"ts":1681786071000}
```

Because the time window is aligned to natural time, the 500-millisecond window will trigger at 500, 1000 and 1500, etc. times of 500 millisecond. The sample data is relatively short, so it only triggers at 500 and 1000, and calculates the average value.

### More merge algorithms

The above are some of the most common merge algorithms. If you have better merge algorithms and unique merge scenarios, please discuss in [GitHub Discussions](https://github.com/lf-edge/ekuiper/discussions/categories/use-case).
