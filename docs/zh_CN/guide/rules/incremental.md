# 增量计算

在使用 eKuiper 对窗口内的数据进行聚合函数计算时，之前的实现方法是将源源不断的流数据按照窗口定义切分成窗口，并缓存在内存中。当窗口结束后，再将窗口内的所有数据进行聚合计算。该方法所带来的一个问题是当数据还未被聚合计算时，缓存在内存里容易造成内存放大，引起 OOM 问题。

目前，eKuiper 支持了对窗口内的聚合函数进行增量计算，只要该聚合函数支持增量计算。当流数据进入窗口时，聚合函数的增量计算将会对该数据进行处理并计算成一个中间状态，从而无需再将整条数据缓存在内存中。

我们可以通过以下[函数列表](../../sqls/functions/aggregate_functions.md)查询哪些聚合函数支持增量计算。

## 启用增量计算

对于以下场景，我们在一个窗口内用 `count` 来进行聚合计算:

```json
{
  "id": "rule",
  "sql": "SELECT count(*) from demo group by countwindow(4)",
  "actions": [
    {
      "log": {}
    }
  ],
  "options": {}
}
```

对于以上规则，我们可以通过 [explain api](../../api/restapi/rules.md#查询规则计划) 来查询规则的查询计划:

```txt
{"op":"ProjectPlan_0","info":"Fields:[ Call:{ name:count, args:[*] } ]"}
    {"op":"WindowPlan_1","info":"{ length:4, windowType:COUNT_WINDOW, limit: 0 }"}
            {"op":"DataSourcePlan_2","info":"StreamName: demo"}
```

通过上述查询计划，我们可以了解到上述规则在实际运行时，会将数据缓存在内存中，等窗口结束后再进行计算，这可能会导致内存消耗过大。

我们可以通过在 `options` 中启用增量计算，以以下规则为例:

```json
{
  "id": "rule",
  "sql": "SELECT count(*) from demo group by countwindow(4)",
  "actions": [
    {
      "log": {}
    }
  ],
  "options": {
    "planOptimizeStrategy": {
      "enableIncrementalWindow": true
    }
  }
}
```

然后查看查询计划:

```txt
{"op":"ProjectPlan_0","info":"Fields:[ Call:{ name:bypass, args:[$$default.inc_agg_col_1] } ]"}
    {"op":"IncAggWindowPlan_1","info":"wType:COUNT_WINDOW, funcs:[Call:{ name:inc_count, args:[*] }->inc_agg_col_1]"}
            {"op":"DataSourcePlan_2","info":"StreamName: demo, StreamFields:[ inc_agg_col_1 ]"}
```

通过上述查询计划，可以发现在该规则运行时，它的计划从 `WindowPlan` 改变为了 `IncAggWindowPlan`, 这代表了数据进入该窗口后会直接进行计算，而非缓存在内存内。

## 无法使用增量计算的场景

当存在某一个聚合函数本身无法被增量计算时，即使打开了增量计算也没有作用，如下述规则所示:

```json
{
  "id": "rule",
  "sql": "SELECT count(*), stddev(a) from demo group by countwindow(4)",
  "actions": [
    {
      "log": {}
    }
  ],
  "options": {
    "planOptimizeStrategy": {
      "enableIncrementalWindow": true
    }
  }
}
```

查看查询计划:

```txt
{"op":"ProjectPlan_0","info":"Fields:[ Call:{ name:count, args:[*] }, Call:{ name:stddev, args:[demo.a] } ]"}
    {"op":"WindowPlan_1","info":"{ length:4, windowType:COUNT_WINDOW, limit: 0 }"}
            {"op":"DataSourcePlan_2","info":"StreamName: demo"}
```

可以看到由于 `stddev` 是一个不支持增量计算的聚合函数，所以这个规则的查询计划中并没有打开增量计算。
