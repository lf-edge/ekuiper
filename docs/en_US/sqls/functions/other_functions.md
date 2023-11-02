## Other Functions

The following function are built-in functions that are not included in other document pages.

## ISNULL

```text
isNull(col)
```

Returns true if the argument is the null value.

## COALESCE

```text
coalesce(expr1, expr2, ...)
```

Return the first non-null value. If all expressions are null,return null.

## NEWUUID

```text
newuuid()
```

Returns a random 16-byte UUID.

## TSTAMP

```text
tstamp()
```

Returns the current timestamp in milliseconds from 00:00:00 Coordinated Universal Time (UTC), Thursday, 1 January 1970.

## EVENT_TIME

```text
event_time()
```

Returns the int64 timestamp of the current processing event.
It may be earlier then the current time due to processing
latency.

If it is used in a window rule as aggregate function, it returns the window end time.

## RULE_ID

```text
rule_id()
```

Returns the ID of the currently matched rule.

## RULE_START

```text
rule_start()
```

Returns the rule start timestamp in int64 format.

## MQTT

```text
mqtt(topic)
```

Returns the metadata of the MQTT message. The same as meta function but can only be used when the rule is triggered by
MQTT message.

## META

```text
meta(topic)
```

Returns the meta-data of a specified key. The key could be:

- A standalone key if there is only one source in the form clause, such as `meta(device)`
- A qualified key to specify the stream, such as `meta(src1.device)`
- A key to refer to nested field for multi level metadata, such as `meta(src1.reading.device.name)`. This assumes
  reading is map structure metadata.

## LAST_HIT_COUNT

```text
last_hit_count()
```

Returns the number of times the function had been called and passed.
The function is usually used to get the accumulated trigger count of a continuous rule.
If the function is used in `WHERE` clause, it will only update the count when the condition is true.

Notice that, this function is not supported in aggregate rule except using in `WHEN` clause of a sliding window.
To get the hit count of an aggregate rule, use [last_agg_hit_count](./aggregate_functions.md#last_agg_hit_count) instead.

If used in a sliding window trigger condition,
the status will be updated only when the trigger condition is met regardless of the rule trigger result.

## LAST_HIT_TIME

```text
last_hit_time()
```

Returns the int64 timestamp of the last **event** time the function had been called and passed.
The function is usually used to get the last trigger time of a continuous rule.
If the function is used in `WHERE` clause, it will only update the timestamp when the condition is true.

Notice that, this function is not supported in aggregate rule except using in `WHEN` clause of a sliding window.
To get the hit time of an aggregate rule, use [last_agg_hit_time](./aggregate_functions.md#last_agg_hit_time) instead.

If used in a sliding window trigger condition,
the status will be updated only when the trigger condition is met regardless of the rule trigger result.

## WINDOW_START

```text
window_start()
```

Return the window start timestamp in int64 format. If there is no time window, it returns 0. The window time is aligned
with the timestamp notion of the rule. If the rule is using processing time, then the window start timestamp is the
processing timestamp. If the rule is using event time, then the window start timestamp is the event timestamp.

## WINDOW_END

```text
window_end()
```

Return the window end timestamp in int64 format. If there is no time window, it returns 0. The window time is aligned
with the timestamp notion of the rule. If the rule is using processing time, then the window end timestamp is the
processing timestamp. If the rule is using event time, then the window end timestamp is the event timestamp.

## GET_KEYED_STATE

```text
get_keyed_state(key, dataType, defaultValue)
```

Return the keyed value in the database. The First parameter is the key, the second is the data type of the value,
support bigint, float, string, boolean and datetime. Third is the default value if key does not exist. Default database
is sqlite, users can change the database by
this [configuration](../../configuration/global_configurations.md#external-state).

## DELAY

```text
delay(delayTime, returnVal)
```

Delay the execution of the rule for a specified time and then return the returnVal. DelayTime is an integer in
milliseconds.
