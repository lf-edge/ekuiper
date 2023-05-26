# 分析函数

分析函数会保持状态来做分析工作。在流式处理规则中，分析函数会首先被执行，这样它们就不会受到 WHERE 子句的影响而必不更新状态。

分析函数完整使用格式如下，其中 over 子句为可选子句。

```
AnalyticFuncName(<arguments>...) OVER ([PARTITION BY <partition key>] [WHEN <Expression>])
```

分析函数的计算是在当前查询输入的所有输入事件上进行的，可以选择限制分析函数只考虑符合 PARTITION BY 子句的事件。
分析函数可以使用 PARTITION BY 子句，语法如下：

```
AnalyticFuncName(<arguments>...) OVER ([PARTITION BY <partition key>])
```

分析函数可以使用 WHEN 条件判断子句，根据是否满足条件来确定当前事件是否为有效事件。
当为有效事件时，根据分析函数语意计算结果并更新状态。当为无效事件时，忽略事件值，复用保存的状态值。

```
AnalyticFuncName(<arguments>...) OVER ([WHEN <Expression>])
```

## LAG

```
lag(expr, [offset], [default value])
```

返回表达式前一个值在偏移 offset 处的结果，如果没有找到，则返回默认值，如果没有指定默认值则返回 nil。

示例1：获取之前温度值的函数

```text
lag(temperature)
```

示例2：获取相同设备之前温度值的函数

```text
lag(temperature) OVER (PARTITION BY deviceId)
```

示例3：ts为时间戳，获取设备状态 statusCode1 和 statusCode2 不相等持续时间

```text
select lag(Status) as Status, ts - lag(ts, 1, ts) OVER (WHEN had_changed(true, statusCode)) as duration from demo
```

## LATEST

```
latest(expr, [default value])
```

返回表达式最新的非空值。如果没有找到，则返回默认值。否则，返回 nil 。

## CHANGED_COL

```
changed_col(true, col)
```

返回列的相比上次执行后的变化值。若未变化则返回 null 。

## HAD_CHANGED

```
had_changed(true, expr1, expr2, ...)
```

返回是否上次运行后列的值有变化。 其参数可以为 * 以方便地监测所有列。

## 监控变化的函数

### Changed_col 函数

该函数为普通的标量函数，因此可在任意的子句，包括 SELECT 和 WHERE 中使用。

**语法**

```CHANGED_COL(<ignoreNull>, <expr>)```

**参数**

**ignoreNull**:  判断变化时是否忽略 null 值。若为 true，则收到 null 值或未收到值不会触发变化。

**expr**: 用来监控变化状态和输出变化值的表达式。

**返回值**

返回变化后的值或者 null （未变化）。与所有标量函数相同，该函数默认返回的列名未函数的名字 changed_col 。可使用 `as alias` 赋别名。

### Changed_cols 函数

该函数返回多个列的结果，因此只能在 SELECT 子句中使用。

**语法**

```CHANGED_COLS (<prefix>, <ignoreNull>, <expr> [,...,<exprN>])```

**参数**

**prefix**: 返回的列名的前缀。默认情况下，返回的变化列名与原列名相同，例如 `CHANGED_COLS("", true, col1)` 返回 `col1`
。如果设置了前缀参数，则返回的列名将加上前缀以区别于普通的列，例如 `CHANGED_COLS("changed_", true, col1)`
将返回 `changed_col1`。

**ignoreNull**: 判断变化时是否忽略 null 值。若为 true，则收到 null 值或未收到值不会触发变化。

**expr**: 用来监控变化状态和输出变化值的表达式。可以为任何可在 SELECT 子句中使用的表达式。若表达式为 `*` 则会返回所有列的变化。

**返回值**

返回所有与上一次运行的值有变化的表达式的新值。如果在普通规则中使用，则与上次事件触发时的值比较。如果在窗口规则中使用，则与上次窗口输出的值比较。

首次运行时，返回所有表达式的值，因为没有前一次的运行，所有表达式都判定为有变化。

在接下来的运行中，如果选择的所有表达式都没有值变化，则返回空值。

**注意事项**

多列函数仅可在 select 子句中使用。其选出的值不能用于 WHERE 或其他子句中。若需要根据变化值做过滤，则应使用 CHANGED_COL
函数，或者将 CHANGED_COLS 的规则作为规则流水线的前置规则。

函数返回的列命别名仅能通过 prefix 参数做全局的设置。若需要给每个列设置单独的别名，则需要使用 CHANGED_COL 函数。

### Had_changed 函数

该函数为向量函数，支持不定长度参数。

```HAD_CHANGED (<ignoreNull>, <expr> [,...,<exprN>])```

**参数**

**ignoreNull**: 判断变化时是否忽略 null 值。若为 true，则收到 null 值或未收到值不会触发变化。

**expr**: 用来监控变化状态和输出变化值的表达式。可以为任何可在 SELECT 子句中使用的表达式。若表达式为 `*` 则监测所有列的变化。

**返回值**

返回一个 bool 值，表示上次运行后的变化状态。多参数版本与用或连接使用单个参数的版本相同，即 HAD_CHANGED(expr1) OR
HAD_CHANGED(expr2) ... OR HAD_CHANGED(exprN) 。若需要监测别的关系，可单独使用此函数。例如，监测是否所有值都有变化，可使用
HAD_CHANGED(expr1) AND HAD_CHANGED(expr2) ... AND HAD_CHANGED(exprN) 。

### 范例

创建流 demo，并给与如下输入。

```json lines
{
  "ts": 1,
  "temperature": 23,
  "humidity": 88
}
{
  "ts": 2,
  "temperature": 23,
  "humidity": 88
}
{
  "ts": 3,
  "temperature": 23,
  "humidity": 88
}
{
  "ts": 4,
  "temperature": 25,
  "humidity": 88
}
{
  "ts": 5,
  "temperature": 25,
  "humidity": 90
}
{
  "ts": 6,
  "temperature": 25,
  "humidity": 91
}
{
  "ts": 7,
  "temperature": 25,
  "humidity": 91
}
{
  "ts": 8,
  "temperature": 25,
  "humidity": 91
}
```

获取 temperature 变化值的规则:

```text
SQL: SELECT CHANGED_COLS("", true, temperature) FROM demo
___________________________________________________
{"temperature":23}
{"temperature":25}
```

获取 temperature 或 humidity 的变化值并添加名称前缀的规则:

```text
SQL: SELECT CHANGED_COLS("c_", true, temperature, humidity) FROM demo
_________________________________________________________
{"c_temperature":23,"c_humidity":88}
{"c_temperature":25}
{"c_humidity":90}
{"c_humidity":91}
```

获取所有列的变化值并且不忽略 null 值的规则:

```text
SQL: SELECT CHANGED_COLS("c_", false, *) FROM demo
_________________________________________________________
{"c_ts":1, "c_temperature":23, "c_humidity":88}
{"c_ts":2}
{"c_ts":3}
{"c_ts":4, "c_temperature":25}
{"c_ts":5, "c_humidity":90}
{"c_ts":6, "c_humidity":91}
{"c_ts":7}
{"c_ts":8}
```

获取窗口中平均值变化的规则:

```text
SQL: SELECT CHANGED_COLS("t", true, avg(temperature)) FROM demo GROUP BY CountWindow(2)
_________________________________________________________________
{"tavg":23}
{"tavg":24}
{"tavg":25}
```

当 temperature 或者 humidity 变化时获取数据:

```text
SQL: SELECT ts, temperature, humidity FROM demo
WHERE HAD_CHANGED(true, temperature, humidity) = true
_________________________________________________________
{"ts":1,temperature":23,"humidity":88}
{"ts":4,temperature":25,"humidity":88}
{"ts":5,temperature":25,"humidity":90}
{"ts":6,temperature":25,"humidity":91}
```

当 temperature 变化且 humidity 未变化时获取数据:

```text
SQL: SELECT ts, temperature, humidity FROM demo 
WHERE HAD_CHANGED(true, temperature) = true AND HAD_CHANGED(true, humidity) = false
_________________________________________________________
{"ts":4,temperature":25,"humidity":88}
```

获取 temperature 和 humidity 的变化值并赋自定义名:

```text
SQL: SELECT CHANGED_COL(true, temperature) AS myTemp, CHANGED_COL(true, humidity) AS myHum FROM demo
_________________________________________________________
{"myTemp":23,"myHum":88}
{"myTemp":25}
{"myHum":90}
{"myHum":91}
```

当 temperature 值变化后大于 24 时获取数据:

```text
SQL: SELECT ts, temperature, humidity FROM demo 
WHERE CHANGED_COL(true, temperature) > 24
_________________________________________________________
{"ts":4,temperature":25,"humidity":88}
```