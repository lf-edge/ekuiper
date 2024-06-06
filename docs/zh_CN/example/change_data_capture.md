# 电流变化时的查询

在物联网场景中，当指标变化时然后进行事件触发是一个非常常见的场景。本篇文章将以电流变化为例，介绍 eKuiper SQL 规则。

## 背景

在该场景中，stream 会以流的形式不断发送当前电流的数据，以及该数据所属的时间戳。eKuiper 规则将接受该数据流，并以定义的规则来满足需求, 数据如下所示:

```json
{"concurrency":200,"ts":1}
{"concurrency":400,"ts":2}
{"concurrency":300,"ts":3}
{"concurrency":200,"ts":4}
```

### 电流由小于300A变为大于300A

```sql
select concurrency, ts from demo where concurrency > 300 and lag(concurrency) < 300;
```

该规则会记录电流上一次的数据，然后和当前数据进行比对，一旦满足条件，则进行事件触发。

```json
{"concurrency":200,"ts":1}
{"concurrency":400,"ts":2}  事件触发
{"concurrency":300,"ts":3}
{"concurrency":200,"ts":4}
```

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
