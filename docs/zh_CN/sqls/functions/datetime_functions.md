# 日期和时间函数

日期和时间函数用于对日期和时间类型数据做操作。

## NOW

```text
now(fsp)
```

按照 `YYYY-MM-DD HH:mm:ss` 返回当前时间，如果提供了 `fsp` 参数来指定从 0 到 6 的小数秒精度，则返回值包括该数字所对应的小数秒部分。

## CURRENT_TIMESTAMP

```text
current_time(fsp)
```

`NOW` 函数的同义词。

## LOCAL_TIME

```text
local_time(fsp)
```

`NOW` 函数的同义词。

## LOCAL_TIMESTAMP

```text
local_timestamp(fsp)
```

`NOW` 函数的同义词。

## CUR_DATE

```text
cur_date()
```

按照 `YYYY-MM-DD` 的格式返回当前日期。

## CURRENT_DATE

```text
current_date()
```

`CUR_DATE` 的同义词。

## CUR_TIME

```text
cur_time()
```

按照 `HH:mm:ss` 的格式返回当前时间。

## CURRENT_TIME

```text
current_time()
```

`CUR_TIME` 的同义词。

## FORMAT_TIME

```text
format_time(time, format)
```

按照 `format` 格式化 `time`，返回格式化后的字符串。

## DATE_CALC

```text
date_calc(date, duration)
```

按照 `date` 和 `duration` 计算日期，返回计算后的日期。`duration` 表示一段时间间隔，可以通过字符串来表示，支持的字符串表示形式如下：

- 纳秒（nanoseconds）：以 "ns" 作为后缀。
- 微秒（microseconds）：以 "us" 或 "µs"（使用 U+00B5，即微符号）作为后缀。
- 毫秒（milliseconds）：以 "ms" 作为后缀。
- 秒（seconds）：以 "s" 作为后缀。
- 分钟（minutes）：以 "m" 作为后缀。
- 小时（hours）：以 "h" 作为后缀。

还支持通过组合来表示更复杂的时间间隔，例如 `1h30m` 表示 1 小时 30 分钟。在表示中，多个时间单位可以连续组合使用，它们之间不需要空格。

如果需要实现减去一段时间间隔，可以在 `duration` 前面加上 `-` 符号，例如 `-1h30m` 表示减去 1 小时 30 分钟。

以下是一些示例：

```text
date_calc('2019-01-01', '1h')
date_calc('2019-01-01', '1h30m')
date_calc('2019-01-01', '1h30m10s')
date_calc('2019-01-01', '1h30m10s100ms')
date_calc('2019-01-01', '1h30m10s100ms200us')
date_calc('2019-01-01', '1h30m10s100ms200us300ns')
```

## DATE_DIFF

```text
date_diff(date1, date2)
```

计算 `date1` 和 `date2` 之间的天数差，返回计算后的天数差。

## DAY_NAME

```text
day_name(date)
```

返回 `date` 所在的天的名称，例如 `Monday`、`Tuesday` 等。

## DAY_OF_MONTH

```text
day_of_month(date)
```

返回 `date` 所在的月份的第几天。

## DAY

```text
day(date)
```

`DAY_OF_MONTH` 的同义词。

## DAY_OF_WEEK

```text
day_of_week(date)
```

返回 `date` 所在的星期的第几天，星期天为 1，星期一为 2，以此类推。

## DAY_OF_YEAR

```text
day_of_year(date)
```

返回 `date` 所在的年份的第几天。

## FROM_DAYS

```text
from_days(days)
```

将 `days` 转换为日期，返回转换后的日期。

## FROM_UNIX_TIME

```text
from_unix_time(unix_timestamp)
```

将 `unix_timestamp` 转换为日期，返回转换后的日期。

## HOUR

```text
hour(date)
```

返回 `date` 的小时部分。

## LAST_DAY

```text
last_day(date)
```

返回 `date` 所在月份的最后一天。

## MICROSECOND

```text
microsecond(date)
```

返回 `date` 的微秒部分。

## MINUTE

```text
minute(date)
```

返回 `date` 的分钟部分。

## MONTH

```text
month(date)
```

返回 `date` 的月份部分。

## MONTH_NAME

```text
month_name(date)
```

返回 `date` 的月份名称，例如 `January`、`February` 等。

## SECOND

```text
second(date)
```

返回 `date` 的秒部分。
