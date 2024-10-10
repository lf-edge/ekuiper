# Notify when current changes

In IoT scenarios, it is a very common scenario to trigger events when indicators change. This article will use current changes as an example to introduce eKuiper SQL rules.

## Background

In this scenario, stream will continuously send the current current data in the form of a stream, as well as the
timestamp to which the data belongs. In this document, we will use the sample input data as follows:

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

### Trigger When Changed Value Pass Threshold

In IoT applications, users often need to monitor whether sensor values exceed a certain threshold, thereby triggering
alarms or other actions. Simply comparing the current value with the threshold may lead to continuous triggering of
alarms. Therefore, what users might actually need is to trigger an alarm when the value changes from not exceeding the
threshold to exceeding it, which implies a process of judging the change. Let's check how eKuiper can help to fulfill
this requirement.

#### 1. Changed current value exceeds 300

```sql
select current, ts
from demo
where current > 300 and lag(current) <= 300;
```

This rule will record the last data of the current, and then compare it with the current data. Once the conditions are
met, the event will be triggered.

```json
{"current":400,"ts":2}
{
  "current": 500,
  "ts": 5
}
{
  "current": 400,
  "ts": 7
}
```

Notice that, this rule will check all deviceIds all together. If you need to separate devices, checkout the next
scenario.

#### 2. Changed current value of a deviceId exceeds 300

```sql
select current, deviceId, ts
from demo
where current > 300 and lag(current) over (partition by deviceId) < 300;
```

This rule will record lag value partition by device. Thus the output will be:

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

Although the input stream mixes data from multiple devices, we can still calculate the lag value separately.

#### 3. Changed value of a specific device

If users only care about a specific device, we can use OVER when clause to only calculate state of concerned device.

```sql
select current, deviceId, ts
from demo
where current > 300 and deviceId = 1 and lag(current) over (when deviceId = 1) < 300;
```

The output will be:

```json
{
  "current": 500,
  "ts": 5,
  "deviceId": 1
}
```

In this rule, the where clause has condition `deviceId = 1` to specify the deviceId. And in the lag function, over when
clause limits the lag value to only record when `deviceId=1`. This will only capture changes of device 1, regardless of
other devices in the same stream.

Besides `lag` function, other analytic functions like had_changed also supports the OVER clause to limit the state
dimension. Check [analytic functions](../sqls/functions/analytic_functions.md) for detail.

### Trigger When Passing Threshold for Some Time

```sql
select current from demo group by SLIDINGWINDOW(ss,0,10) over (when current > 200) having min(current) > 200;
```

This rule will open a window when receiving current data above 200A. If the smallest data in the window is also greater than 200A, the requirements are met and the event is output.

```json
{"current":100,"ts":1}
{"current":300,"ts":2}
{"current":300,"ts":3}
{"current":300,"ts":4}
{"current":300,"ts":5}
{"current":300,"ts":6}
{"current":300,"ts":7}
{"current":300,"ts":8}
{"current":300,"ts":9}
{"current":300,"ts":10}
{
  "current": 300,
  "ts": 11
} Output Event
```
