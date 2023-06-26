# Analytic Functions

Analytic functions use state to do analytic jobs. In streaming processing, analytic functions are evaluated first so
that they are not affected by predicates in WHERE clause.

Analytic function call format is as below, where `over` clause is optional

```text
AnalyticFuncName(<arguments>...) OVER ([PARTITION BY <partition key>] [WHEN <Expression>])
```

Analytic function computations are performed over all the input events of the current query input, optionally you can
limit analytic function to only consider events that match the partition_by_clause.

The syntax is like:

```text
AnalyticFuncName(<arguments>...) OVER ([PARTITION BY <partition key>])
```

The analysis function can use the WHEN clause to determine whether the current event is a valid event based on whether
the condition is met.
When it is a valid event, calculate the result and update the state according to the analysis function semantics. When
it is an invalid event, ignore the event value and reuse the saved state value.

```text
AnalyticFuncName(<arguments>...) OVER ([WHEN <Expression>])
```

## LAG

```text
lag(expr, [offset], [default value])
```

Return the former result of expression at offset, if not found, return the default value specified, if default value not
set, return nil. If offset and default value are not specified, offset is 1 and default value is nil.

Example function call to get the previous temperature value:

```text
lag(temperature)
```

Example function call to get the previous temperature value with the same device id:

```text
lag(temperature) OVER (PARTITION BY deviceId)
```

Example function call to calculate duration of events: ts is timestamp, and statusCode1 and statusCode2 are device
status in the same event

```text
select lag(Status) as Status, ts - lag(ts, 1, ts) OVER (WHEN had_changed(true, statusCode)) as duration from demo
```

## LATEST

```text
latest(expr, [default value])
```

Return the latest non-null value of the expression. If not found, return the default value specified, if default value
not set, return nil.

## CHANGED_COL

```text
changed_col(true, col)
```

Return the column value if it has changed from the last execution.

## HAD_CHANGED

```text
had_changed(true, expr1, expr2, ...)
```

Return if any of the columns had changed since the last run. The expression could be * to easily detect the change
status of all columns.

## Functions to detect changes

### Changed_col function

This function is a normal scalar function, so it can be used in any clause including SELECT and WHERE.

**Syntax**

```CHANGED_COL(<ignoreNull>, <expr>)```

**Arguments**

**ignoreNull**: whether to ignore null values when comparing for changes. If true, the null value won’t emit a change.

**expr**: An expression to be selected and monitored for the changed status.

**Returns**

Return the changed value or nil with column name changed_col by default like any other functions. Use `as alias` to
rename the column.

### Changed_cols function

This function returns multiple columns, so it is only allowed in the SELECT clause.

**Syntax**

```CHANGED_COLS (<prefix>, <ignoreNull>, <expr> [,...,<exprN>])```

**Arguments**

**prefix**: The prefix of the selected column name. By default, the selected name will be the same as select the expr
directly. For example, `CHANGED_COLS("", true, col1)` will return `col1` as the name. If setting a prefix, the return
name will have that prefix. For example, `CHANGED_COLS("changed_", true, col1)` will return `changed_col1` as the name.

**ignoreNull**: whether to ignore null values when detecting changes. If true, the null value won’t trigger a change.

**expr**: An expression to be selected and monitored for the changed status. Allow any expression that can be used in
select clause. The expression can be a `*` which will return multiple columns by one expression.

**Returns**

Return all changed values compared to the previous sink result. So if used in a scalar rule, it will compare to the
previous value emitting. If used in a window, it will compare to the previous window result.

In the first run, all expressions will be returned because there is no previous result.

In the consequent runs, if nothing changed, it can emit nothing. And if the sink has the default omitEmpty, the sink
will not be triggerred.

**Notice**

The multiple column outputs can only be used in the select clause. Even the selected result cannot be accessed in WHERE
or other place. If a filter based on the value is needed, use CHANGED_COL or set the result of multiple column outputs
as the prior rule in a rule chain.

For multiple column outputs, the alias can only be set generally with the prefix. To set alias for each column
separately, try to call the changed function for each column respectively and use as to set alias.

### Had_changed function

This function is a scalar function with one or more arguments.

```HAD_CHANGED (<ignoreNull>, <expr> [,...,<exprN>])```

**Arguments**

**ignoreNull**: whether to ignore null values when detecting changes. If true, the null value won’t trigger a change.

**expr**: An expression to be monitored for the changed status. Allow any expression that can be used in select clause.
The expression can be a `*` to detect changes of all columns easily.

**Returns**

Return a bool value to indicate the changed status if any of the arguments had changed since the last run. The multiple
arguments' version is a handy way to check HAD_CHANGED(expr1) OR HAD_CHANGED(expr2) ... OR HAD_CHANGED(exprN). To detect
another relationship, just use separate HAD_CHANGED functions. For example, to check if all expressions are changed
HAD_CHANGED(expr1) AND HAD_CHANGED(expr2) ... AND HAD_CHANGED(exprN).

### Examples

Create a stream demo and have below inputs

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

Rule to get the changed temperature values:

```text
SQL: SELECT CHANGED_COLS("", true, temperature) FROM demo
___________________________________________________
{"temperature":23}
{"temperature":25}
```

Rule to get the changed temperature and humidity values, and rename the changed value in a unified prefix:

```text
SQL: SELECT CHANGED_COLS("c_", true, temperature, humidity) FROM demo
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

Rule to get the changed values of all columns and do not ignore null:

```text
SQL: SELECT CHANGED_COLS("c_", false, *) FROM demo
_________________________________________________________
{"c_temperature":23,"c_humidity":88}
{"c_temperature":25}
{"c_humidity":90}
{"c_humidity":91}
```

Rule to get the average value change in a window:

```text
SQL: SELECT CHANGED_COLS("t", true, avg(temperature)) FROM demo GROUP BY CountWindow(2)
_________________________________________________________________
{"tavg":23}
{"tavg":24}
{"tavg":25}
```

Rule to get the events when temperature or humidity changed:

```text
SQL: SELECT ts, temperature, humidity FROM demo
WHERE HAD_CHANGED(true, temperature, humidity) = true
_________________________________________________________
{"ts":1,temperature":23,"humidity":88}
{"ts":4,temperature":25,"humidity":88}
{"ts":5,temperature":25,"humidity":90}
{"ts":6,temperature":25,"humidity":91}
```

Rule to get the events when temperature has changed but humidity has NOT changed:

```text
SQL: SELECT ts, temperature, humidity FROM demo 
WHERE HAD_CHANGED(true, temperature) = true AND HAD_CHANGED(true, humidity) = false
_________________________________________________________
{"ts":4,temperature":25,"humidity":88}
```

Rule to get the changed temperature and humidity value with customized names:

```text
SQL: SELECT CHANGED_COL(true, temperature) AS myTemp, CHANGED_COL(true, humidity) AS myHum FROM demo
_________________________________________________________
{"myTemp":23,"myHum":88}
{"myTemp":25}
{"myHum":90}
{"myHum":91}
```

Rule to get the changed values when the temperature had changed to value bigger than 24:

```text
SQL: SELECT ts, temperature, humidity FROM demo 
WHERE CHANGED_COL(true, temperature) > 24
_________________________________________________________
{"ts":4,temperature":25,"humidity":88}
```
