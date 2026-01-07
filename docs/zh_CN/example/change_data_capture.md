# 电流变化时的查询

在物联网场景中，当指标变化时然后进行事件触发是一个非常常见的场景。本篇文章将以电流变化为例，介绍 eKuiper SQL 规则。

## 背景

在该场景中，stream 会以流的形式不断发送当前电流的数据，以及该数据所属的时间戳。本文中，我们将使用如下模拟输入数据:

```json
{
  "current": 300,
  "ts": 1,
  "deviceId": 1
}
{
  "current": 400,
  "ts": 2,
  "deviceId": 2
}
{
  "current": 200,
  "ts": 3,
  "deviceId": 1
}
{
  "current": 200,
  "ts": 4,
  "deviceId": 2
}
{
  "current": 500,
  "ts": 5,
  "deviceId": 1
}
{
  "current": 200,
  "ts": 6,
  "deviceId": 2
}
{
  "current": 400,
  "ts": 7,
  "deviceId": 1
}
{
  "current": 600,
  "ts": 8,
  "deviceId": 2
}
```

### 当值变化为超过阈值时触发

在物联网应用中，用户经常需要监控传感器数值是否超过某个阈值，从而触发报警等动作。通过简单的比较当前值与阈值大小，可能会导致报警的持续触发。因此，用户可能更需要的是当数值由不超过阈值变为超过阈值时触发报警，其中隐含了判断变化的过程。以下为几个常见的场景：

#### 1. 电流变化后超过阈值

```sql
select concurrency, ts
from demo
where concurrency > 300
  and lag(concurrency) <= 300;
```

该规则会记录电流上一次的数据，然后和当前数据进行比对，一旦满足条件，则进行事件触发。

```json
{
  "current": 400,
  "ts": 2
}
{
  "current": 500,
  "ts": 5
}
{
  "current": 400,
  "ts": 7
}
```

请注意，该规则会检测整个数据流（包含所有设备的值）的变化。如果需要区分设备，请看下面的场景。

#### 2. 各设备的电流变化后超过阈值

```sql
select current, deviceId, ts
from demo
where current > 300 and lag(current) over (partition by deviceId) < 300;
```

该规则会记录每个设备的上一次的数据与该设备当前数据进行比较。输出结果如下：

```json
{
  "current": 500,
  "ts": 5,
  "deviceId": 1
}
{
  "current": 600,
  "ts": 8,
  "deviceId": 2
}
```

可见，在输入数据包含了多个设备的电流值的情况下，我们仍然可以基于设备进行触发。

#### 3. 某个设备的电流变化后超过阈值

假设用户只关心某个具体的设备如 deviceId 为 1 的设备，我们可以通过 OVER WHEN 语句来限定 lag 的范围。

```sql
select current, deviceId, ts
from demo
where current > 300 and deviceId = 1 and lag(current) over (when deviceId = 1) < 300;
```

输出结果如下:

```json
{
  "current": 500,
  "ts": 5,
  "deviceId": 1
}
```

在此规则中，WHERE 语句里添加了条件 `deviceId = 1`，这样只会针对该设备进行计算。另外在 lag 函数中，OVER WHEN 条件同样限定了
deviceId，这样与当前设备值比较的只会是该设备上一次的值，排除了数据流中其余设备数据的影响。

除了 `lag` 函数，其余分析函数，例如 had_changed 等都支持 OVER
子句来限定状态的维度。详细信息请查看[分析函数](../sqls/functions/analytic_functions.md)。

### 总电流持续10s超过200A

```sql
select concurrency from demo group by SLIDINGWINDOW(ss,0,10) over (when concurrency > 200) having min(concurrency) > 200;
```

该规则会当接收到 200A 以上的电流数据时开启一个窗口，如果该窗口中最小的数据也大于 200A ，则满足需求然后输出事件

```json
{"concurrency":100,"ts":1}
{"concurrency":300,"ts":2}
{"concurrency":300,"ts":3}
{"concurrency":300,"ts":4}
{"concurrency":300,"ts":5}
{"concurrency":300,"ts":6}
{"concurrency":300,"ts":7}
{"concurrency":300,"ts":8}
{"concurrency":300,"ts":9}
{"concurrency":300,"ts":10}
{"concurrency":300,"ts":11} 输出事件
```
