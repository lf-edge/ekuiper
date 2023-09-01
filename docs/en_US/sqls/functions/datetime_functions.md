# Date and Time Functions

Date and time functions are used to perform operations on date and time type data.

## NOW

```text
now(fsp)
```

Returns the current time in the `YYYY-MM-DD HH:mm:ss` format. If the `fsp` parameter is provided to specify fractional seconds precision from 0 to 6, the returned value includes the corresponding fraction of seconds.

## CURRENT_TIMESTAMP

```text
current_time(fsp)
```

Synonym for the `NOW` function.

## LOCAL_TIME

```text
local_time(fsp)
```

Synonym for the `NOW` function.

## LOCAL_TIMESTAMP

```text
local_timestamp(fsp)
```

Synonym for the `NOW` function.

## CUR_DATE

```text
cur_date()
```

Returns the current date in the `YYYY-MM-DD` format.

## CURRENT_DATE

```text
current_date()
```

Synonym for the `CUR_DATE` function.

## CUR_TIME

```text
cur_time()
```

Returns the current time in the `HH:mm:ss` format.

## CURRENT_TIME

```text
current_time()
```

Synonym for the `CUR_TIME` function.

## FORMAT_TIME

```text
format_time(time, format)
```

Formats the `time` according to the specified `format` and returns the formatted string.

## DATE_CALC

```text
date_calc(date, duration)
```

Calculates the date based on the `date` and `duration` and returns the calculated date.

The `duration` represents a time interval and can be represented as a string using the following formats:

- Nanoseconds (`ns`): Suffixed with "ns".
- Microseconds (`us` or `µs`): Suffixed with "us" or "µs" (using U+00B5 micro symbol).
- Milliseconds (`ms`): Suffixed with "ms".
- Seconds (`s`): Suffixed with "s".
- Minutes (`m`): Suffixed with "m".
- Hours (`h`): Suffixed with "h".

It also supports combining these representations for more complex time intervals, for example, `1h30m` represents 1 hour 30 minutes. Multiple time units can be combined without spaces.

To subtract a time interval, you can prepend a `-` sign before the `duration`.

For example, `-1h30m` represents subtracting 1 hour 30 minutes.

Here are some examples for the `duration`：

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

Calculates the difference in days between `date1` and `date2` and returns the calculated difference.

## DAY_NAME

```text
day_name(date)
```

Returns the name of the day of the week for the given `date`, such as `Monday`, `Tuesday`, etc.

## DAY_OF_MONTH

```text
day_of_month(date)
```

Returns the day of the month for the given `date`.

## DAY

```text
day(date)
```

Synonym for `DAY_OF_MONTH`.

## DAY_OF_WEEK

```text
day_of_week(date)
```

Returns the day of the week for the given `date`, where Sunday is 1, Monday is 2, and so on.

## DAY_OF_YEAR

```text
day_of_year(date)
```

Returns the day of the year for the given `date`.

## FROM_DAYS

```text
from_days(days)
```

Converts the `days` value to a date and returns the converted date.

## FROM_UNIX_TIME

```text
from_unix_time(unix_timestamp)
```

Converts the `unix_timestamp` value to a date and returns the converted date.

## HOUR

```text
hour(date)
```

Returns the hour part of the given `date`.

## LAST_DAY

```text
last_day(date)
```

Returns the last day of the month for the given `date`.

## MICROSECOND

```text
microsecond(date)
```

Returns the microsecond part of the given `date`.

## MINUTE

```text
minute(date)
```

Returns the minute part of the given `date`.

## MONTH

```text
month(date)
```

Returns the month part of the given `date`.

## MONTH_NAME

```text
month_name(date)
```

Returns the name of the month for the given `date`, such as `January`, `February`, etc.

## SECOND

```text
second(date)
```

Returns the second part of the given `date`.
