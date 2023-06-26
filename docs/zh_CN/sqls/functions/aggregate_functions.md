# 聚合函数

聚合函数对一组值执行计算并返回单个值。聚合函数只能用在以下表达式中：

* select 语句的 select 列表（子查询或外部查询）。
* HAVING 子句。

## AVG

```text
avg(col)
```

返回组中的平均值。空值不参与计算。

## COUNT

```text
count(*)
count(col)
```

返回组中的项目数。空值不参与计算。

## MAX

```text
max(col)
```

返回组中的最大值。空值不参与计算。

## MIN

```text
min(col)
```

返回组中的最小值。空值不参与计算。

## SUM

```text
sum(col)
```

返回组中所有值的总和。空值不参与计算。

## COLLECT

```text
collect(*)
collect(col)
```

返回组中指定的列或整个消息（参数为*时）的值组成的数组。

## 示例

* 获取当前窗口所有消息的列 a 的值组成的数组。假设列 a 的类型为 int, 则结果为: `[{"r1":[32, 45]}]`

    ```sql
    SELECT collect(a) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

* 获取当前窗口所有消息的值组成的数组。结果为: `[{"r1":[{"a":32, "b":"hello"}, {"a":45, "b":"world"}]}]`

    ```sql
    SELECT collect(*) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

* 获取当前窗口第二个消息的列 `a` 的值。结果为: `[{"r1":32}]`

    ```sql
    SELECT collect(*)[1]->a as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

## MERGE_AGG

```text
merge_agg(*)
merge_agg(col)
```

将组中的值合并为单个值。若存在重复键时取较后对象的值。它不进行递归操作；只合并顶级键值。

如果参数是列，结果将是一个包含列中所有对象的键的联合的对象。如果列只包含非对象值，则结果将是一个空对象。

## 范例

假设组中的值如下所示：

```json lines
{"a": {"a": 2}, "b": 2, "c": 3}
{"a": {"b": 2}, "b": 5, "d": 6}
{"a": {"a": 3}, "b": 8}
```

* 合并 * 结果为： `{"a": {"a": 3}, "b": 8, "c": 3, "d": 6}`

    ```sql
    SELECT merge_agg(*) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

* 合并对象列，结果为： `{"a": 3, "b": 2}`

    ```sql
    SELECT merge_agg(a) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

* 合并非对象列： `{}`

    ```sql
    SELECT merge_agg(b) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

## DEDUPLICATE

```text
deduplicate(col, false)
```

返回当前组去重的结果，通常用在窗口中。其中，第一个参数指定用于去重的列；第二个参数指定是否返回全部结果。若为 false
，则仅返回最近的未重复的项；若最近的项有重复，则返回空数组；此时可以设置 sink
参数 [omitIfEmpty](../../guide/sinks/overview.md#公共属性)，使得 sink 接到空结果后不触发。

### 示例

* 获取当前窗口中，列 `a` 值不重复的所有消息组成的数组。结果为: `[{"r1":{"a":32, "b":"hello"}, {"a":45, "b":"world"}}]`

    ```sql
    SELECT deduplicate(a, true) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

* 获取列 `a` 值在过去一小时中不重复的值。每收到一条新消息，都会检测列 `a` 是否重复，若不重复则输出：`[{"r1":32}]`
  或者 `[{"r1":45}]`。若检测到重复值，则输出 `[{}]` 。此时，可以设置 omitIfEmpty 的 sink 参数使得检测到重复值时不触发规则。

     ```sql
     SELECT deduplicate(a, false)->a as r1 FROM demo GROUP BY SlidingWindow(hh, 1)
     ```

## STDDEV

```text
stddev(col)
```

返回组中所有值的标准差。空值不参与计算。

## STDDEVS

```text
stddevs(col)
```

返回组中所有值的样本标准差。空值不参与计算。

## VAR

```text
var(col)
```

返回组中所有值的方差。空值不参与计算。

## VARS

```text
vars(col)
```

返回组中所有值的样本方差。空值不参与计算。

## PERCENTILE

```text
percentile(col, 0.5)
```

返回组中所有值的指定百分位数。空值不参与计算。其中，第一个参数指定用于计算百分位数的列；第二个参数指定百分位数的值，取值范围为
0.0 ~ 1.0 。

## PERCENTILE_DISC

```text
percentile_disc(col, 0.5)
```

返回组中所有值的指定百分位数。空值不参与计算。其中，第一个参数指定用于计算百分位数的列；第二个参数指定百分位数的值，取值范围为
0.0 ~ 1.0 。
