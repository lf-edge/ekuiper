## 其他函数

本节介绍未分类到其余文档页面的内置函数。

## ISNULL

```text
isNull(col)
```

如果参数为空值，则返回 true ，否则返回 false 。

## COALESCE

```text
coalesce(expr1, expr2, ...)
```

返回第一个非空参数，如果所有参数都是 null ，则返回 null 。

## NEWUUID

```text
newuuid()
```

返回一个随机的 16 字节 UUID。

## TSTAMP

```text
tstamp()
```

返回当前时间戳，以1970年1月1日星期四00:00:00协调世界时（UTC）为单位。

## RULE_ID

```text
rule_id()
```

返回当前匹配到的规则的ID。

## MQTT

```text
mqtt(topic)
```

返回指定键的 MQTT 元数据。 当前支持的键包括:

- topic：返回消息的主题。 如果有多个流源，则在参数中指定源名称。如 `mqtt(src1.topic)`
- messageid：返回消息的消息ID。如果有多个流源，则在参数中指定源名称。 如 `mqtt(src2.messageid)`

该函数仅用于数据源为 MQTT 的情况。其余数据源请使用 META 函数。

## META

```text
meta(key)
```

返回指定键的元数据。

## WINDOW_START

```text
window_start()
```

返回窗口的开始时间戳，格式为 int64。若运行时没有时间窗口，则返回默认值0。窗口的时间与规则所用的时间系统相同。若规则采用处理时间，则窗口的时间也为处理时间；若规则采用事件事件，则窗口的时间也为事件时间。

## WINDOW_END

```text
window_end()
```

返回窗口的结束时间戳，格式为 int64。若运行时没有时间窗口，则返回默认值0。窗口的时间与规则所用的时间系统相同。若规则采用处理时间，则窗口的时间也为处理时间；若规则采用事件事件，则窗口的时间也为事件时间。

## GET_KEYED_STATE

```text
get_keyed_state(key, state_name)
```

返回键在数据库中对应的值。第一个参数为 键 表达式，第二个参数为值类型，支持 bigint, float, string, boolean and datetime
格式，第三个参数为默认值。默认数据库是sqlite，用户可以通过这个[配置](../../configuration/global_configurations.md#外部状态)
更改数据库。

## DELAY

```text
delay(delayTime, returnVal)
```

延迟执行规则一段时间后返回第二个参数作为返回值。第一个参数为延迟时间，单位为毫秒，第二个参数为返回值。
