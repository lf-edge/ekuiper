# 合并单流多设备数据

动手运行案例，请查看[这里](../howto.md)。

## 问题

在物联网场景中，终端设备如传感器往往数量众多，通常采集软件会将所有设备的数据合并到一个数据流中。由于每个传感器的采集和响应周期不同，数据流中就会间杂各种设备的数据，而且数据较为碎片化，每个事件只包含了一个传感器的数据。例如，传感器A每秒采集一次温度数据，传感器B每5秒采集一次湿度数据，传感器C每10秒采集一次数据，那么数据流中就会有A、B、C三种数据，每种数据的采集频率不同，但都混杂到一起。后端应用中，同一组传感器的设置通常是相关联的，需要将同一组传感器的数据合并到一起，以便后续处理。

## 输入样例

数据流里温湿度传感器数据混杂，且数据都不完整。

```
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

## 期望输出

将同一组传感器（温湿度）的数据合并到一起，以便后续处理。单个事件输出样例如下：

```json
{
  "temperature": 27.23,
   "humidity": 79.66,
  "ts": 1681786070368
}
```

根据不同场景需求，我们可以灵活地编写规则来实现数据合并，控制数据合并的方式，频率，以及合并后的数据输出。

## 解决方案

实际使用中，用户往往有不同的合并算法。本文将列举几种常见的合并算法，以及如何使用规则引擎来实现。

### 1. 每个事件输出一次

这种合并算法是最简单的。每个事件到来时，都获取温湿度的最新数值并组合发出。这种算法的数据输出频率与输入频率相同。

```SQL
SELECT latest(temperature, 0) as temperature, latest(humidity, 0) as humidity, ts FROM demoStream
```

其中，latest(temperature, 0) 会获取最新的温度值。即当前事件中存在温度值，则返回该值；否则返回之前最后收到的温度值，如果之前没有温度值，则返回0。湿度数据同理。通过这个方式，每当收到单温度或单湿度事件时，都会组合成一条包含温湿度的事件发出。

通过这个规则，从样例输入序列中我们可以得到如下输出：

```
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

用户可以根据实际需求，再添加`where`语句对输出做进一步过滤，例如[解决方案3](#3-时间相近的数据合并)中根据时间戳进行了过滤。

### 2. 以温度为准输出

这种合并算法是以温度为主指标，每当收到温度事件时，就获取最新的温度和湿度值并组合发出。这种算法的数据输出频率与温度这个主指标的输入频率相同，湿度作为附属指标仅用于补全数据。

```SQL
SELECT temperature, latest(humidity, 0) as humidity, ts FROM demoStream WHERE isNull(temperature) = false
```

其中，`WHERE isNull(temperature) = false` 会过滤掉不包含温度值的事件。通过这个方式，每当收到温度事件时，都会组合成一条包含温湿度的事件发出。

通过这个规则，从样例输入序列中我们可以得到如下输出：

```
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

### 3. 时间相近的数据合并

这种合并算法基于各传感器数据采集频率相同的假设，时间相近的数据应当包含所有需要的传感器数据，但各个数据收到的时间不固定。以温湿度为例，规则可能先收到温度数据，也可能先收到湿度数据，但是相同批次的数据之间的时间间隔应当接近；相反的是，不同批次的数据之间，时间间隔相对较大。 

```SQL
SELECT latest(temperature, 0) as temperature, latest(humidity, 0) as humidity, ts FROM demoStream WHERE ts - lag(ts) < 10
```

其中，`WHERE timestamp - latest(timestamp, 0) < 10` 会过滤掉时间间隔大于10秒的事件。通过这个方式，每当收到温度或湿度事件时，同一个批次的两个事件，只有第二条事件满足与上一条事件的时间间隔小于10毫秒的条件，才会组合成一条包含温湿度的事件发出。

通过这个规则，从样例输入序列中我们可以得到如下输出：

```
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

### 4. 固定间隔平均值输出

前面几种算法都是以采集所有数据为目标，但是在实际应用中，用户可能不关心每个单独的实时值，而是关心某个指标的变化趋势如平均值。这种情况下，我们可以通过定时触发的方式，每隔一段时间计算一次平均值并输出。我们可以使用 `TUMBLINGWINDOW` 这个时间窗口，每个时间窗口内的数据会被合并成一条数据并进行聚合运算。由于我们的样例数据较短，总共只有1秒的数据，为了能够得到输出，这里我们将时间窗口设置为较短的500毫秒。为了得到固定的结果，我们将采用事件时间的方式进行窗口计算，这样可以保证每个窗口内的数据都是固定的。

```SQL
SELECT avg(temperature) as temperature, avg(humidity) as humidity, window_end() as ts FROM demoStream GROUP BY TUMBLINGWINDOW(ms, 500)
```

其中，`GROUP BY TUMBLINGWINDOW(ms, 500)` 会将每500毫秒的数据合并成一条数据，然后对温度和湿度分别求平均值。通过这个方式，每隔500毫秒，我们就可以得到一条包含温湿度的事件发出。

通过这个规则，从样例输入序列中我们可以得到如下输出：

```
{"humidity":81.75999999999999,"temperature":27.455,"ts":1681786070500}
{"humidity":77.5625,"temperature":27.332500000000003,"ts":1681786071000}
```

由于时间窗口会对齐到自然时间，因此500毫秒的窗口会在 500，1000 和 1500 等500的倍数的时间点触发。样例数据的时间较短，因此只在500 和 1000 两个时间点触发，计算平均值。

### 更多案例

如果您有更好的合并算法以及独特的合并场景，欢迎在 [Github Discussions](https://github.com/lf-edge/ekuiper/discussions/categories/use-case) 讨论。