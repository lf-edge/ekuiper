# Incremental Computation

When using eKuiper to perform aggregate function calculations on data within a window, the previous implementation method was to segment the continuous stream of data according to the window definition and cache it in memory. Once the window ended, all data within the window would be aggregated and calculated. A problem with this method is that before the data is aggregated and calculated, caching it in memory can easily lead to memory amplification, causing OOM (Out of Memory) issues.

Currently, eKuiper supports incremental computation for aggregate functions within a window, provided that the aggregate function supports incremental computation. As stream data enters the window, the incremental computation of the aggregate function will process this data and calculate an intermediate state, thereby eliminating the need to cache the entire data in memory.

We can check which aggregate functions support incremental computation through the [following function list](../../sqls/functions/aggregate_functions.md).

## Enabling Incremental Computation

For the following scenario, we use count for aggregate computation within a window:

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

For the above rule, we can query the rule's execution plan using the [explain api](../../api/restapi/rules.md#query-rule-plan):

```txt
{"op":"ProjectPlan_0","info":"Fields:[ Call:{ name:count, args:[*] } ]"}
    {"op":"WindowPlan_1","info":"{ length:4, windowType:COUNT_WINDOW, limit: 0 }"}
            {"op":"DataSourcePlan_2","info":"StreamName: demo"}
```

We can enable incremental computation in the options, as shown in the following rule example:

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

Then, check the execution plan:

```txt
{"op":"ProjectPlan_0","info":"Fields:[ Call:{ name:bypass, args:[$$default.inc_agg_col_1] } ]"}
    {"op":"IncAggWindowPlan_1","info":"wType:COUNT_WINDOW, funcs:[Call:{ name:inc_count, args:[*] }->inc_agg_col_1]"}
            {"op":"DataSourcePlan_2","info":"StreamName: demo, StreamFields:[ inc_agg_col_1 ]"}
```

From the above execution plan, it can be seen that during the execution of this rule, its plan has changed from `WindowPla`n to `IncAggWindowPlan`, indicating that data entering this window will be directly computed rather than cached in memory.

## Scenarios Where Incremental Computation Cannot Be Used

When there is an aggregate function that inherently cannot be computed incrementally, enabling incremental computation will have no effect, as shown in the following rule:

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

Check the execution plan:

查看查询计划:

```txt
{"op":"ProjectPlan_0","info":"Fields:[ Call:{ name:count, args:[*] }, Call:{ name:stddev, args:[demo.a] } ]"}
    {"op":"WindowPlan_1","info":"{ length:4, windowType:COUNT_WINDOW, limit: 0 }"}
            {"op":"DataSourcePlan_2","info":"StreamName: demo"}
```

It can be seen that since `stddev` is an aggregate function that does not support incremental computation, the execution plan for this rule does not enable incremental computation.
