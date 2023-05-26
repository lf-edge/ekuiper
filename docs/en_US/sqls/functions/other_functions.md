## Other Functions

The following function are built-in functions that are not included in other document pages.

## ISNULL

```
isNull(col)
```

Returns true if the argument is the null value.

## COALESCE

```
coalesce(expr1, expr2, ...)
```

Return the first non-null value. If all expressions are null,return null.

## NEWUUID

```
newuuid()
```

Returns a random 16-byte UUID.

## TSTAMP

```
tstamp()
```

Returns the current timestamp in milliseconds from 00:00:00 Coordinated Universal Time (UTC), Thursday, 1 January 1970.

## RULE_ID

```
rule_id()
```

Returns the ID of the currently matched rule.

## MQTT

```
mqtt(topic)
```

Returns the metadata of the MQTT message. The same as meta function but can only be used when the rule is triggered by
MQTT message.

## META

```
meta(topic)
```

Returns the meta-data of a specified key. The key could be:

- A standalone key if there is only one source in the form clause, such as `meta(device)`
- A qualified key to specify the stream, such as `meta(src1.device)`
- A key to refer to nested field for multi level metadata, such as `meta(src1.reading.device.name)`. This assumes
  reading is map structure metadata.

## WINDOW_START

```
window_start()
```

Return the window start timestamp in int64 format. If there is no time window, it returns 0. The window time is aligned
with the timestamp notion of the rule. If the rule is using processing time, then the window start timestamp is the
processing timestamp. If the rule is using event time, then the window start timestamp is the event timestamp.

## WINDOW_END

```
window_end()
```

Return the window end timestamp in int64 format. If there is no time window, it returns 0. The window time is aligned
with the timestamp notion of the rule. If the rule is using processing time, then the window end timestamp is the
processing timestamp. If the rule is using event time, then the window end timestamp is the event timestamp.

## GET_KEYED_STATE

```
get_keyed_state(key, dataType, defaultValue)
```

Return the keyed value in the database. The First parameter is the key, the second is the data type of the value,
support bigint, float, string, boolean and datetime. Third is the default value if key does not exist. Default database
is sqlite, users can change the database by
this [configuration](../../configuration/global_configurations.md#external-state).

## DELAY

```
delay(delayTime, returnVal)
```

Delay the execution of the rule for a specified time and then return the returnVal. DelayTime is an integer in
milliseconds.