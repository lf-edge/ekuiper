# 扩展分析函数

## CHANGE_CAPTURE

`CHANGE_CAPTURE` 函数用于监控指定值，并在该值变为预定义目标值时捕获（保存）另一个字段的值。如果未满足更改条件，则函数返回上次捕获的值。可选的
`ignoreNull` 参数控制是否忽略监控字段中的 `NULL`
值，以防止其触发捕获。该函数非常适合事件驱动场景，例如在特定条件下需要记录字段（例如时间戳、测量值或状态），并保持上次记录的值直到下一次有效触发。

### 语法

```sql
CHANGE_CAPTURE(save_value, monitor_value, target_value [, ignoreNull])
```

#### 参数

- **save_value**（任意类型）：当满足条件时捕获其值的字段或表达式。可以是时间戳、数字、字符串或数据库支持的任意数据类型。
- **monitor_value**（任意类型）：需要监控变化的字段或表达式。通常是表示状态变化的列（例如状态标志或传感器读数）。
- **target_value**（与 monitor_value 相同类型，可选）：触发捕获 save_value 的常量值，monitor_value 必须等于该值。其数据类型必须与
  monitor_value 匹配。如果未指定，则 monitor_value 的任何变化都会触发捕获。
- **ignoreNull**（布尔值，可选）：如果为 `true`，则忽略 monitor_value 中的 `NULL` 值，函数不会触发捕获或更新状态。如果为
  `false` 或省略，则将 `NULL` 视为与 target_value 比较的有效值。默认值为 `false`。

#### 返回值

- 当 monitor_value 等于 target_value（且 ignoreNull 为 `true` 时 monitor_value 不为 `NULL`）时，返回最近捕获的 save_value。
- 如果 monitor_value 不等于 target_value（或当 ignoreNull 为 `true` 时 monitor_value 为 `NULL`），则返回之前触发捕获的最后
  save_value。
- 如果尚未捕获任何值（例如在流开始时），则返回 `NULL` 或数据库特定的默认值。

### 描述

`CHANGE_CAPTURE` 函数在流式处理环境中运行，保持状态以跟踪上次捕获的值。它适用于以下场景：

- 仅在满足基于另一个字段的条件时记录特定字段。
- 最后记录的值应保持直到再次满足条件。
- 可选择忽略监控字段中的 `NULL` 值以避免不必要的触发。
- 常见用例包括捕获事件时间戳、在传感器状态更改时记录测量值或记录状态转换。

函数逐行评估流中的数据：

1. 如果 monitor_value 为 `NULL` 且 ignoreNull 为 `true`，则跳过该行并返回上次捕获的 save_value。
2. 如果 monitor_value 等于 target_value（且 ignoreNull 为 `true` 时 monitor_value 不为 `NULL`），则捕获当前的 save_value
   并更新存储状态。
3. 如果 monitor_value 不等于 target_value，则返回之前捕获的 save_value（如果有）。
4. 该函数是有状态的，跨行保留上次捕获的值，直到触发新的捕获。

### 示例

#### 示例 1：忽略 NULL 值捕获状态更改的时间戳

**场景**：在物联网系统中，当传感器状态变为 `1`（例如“开启”）时捕获时间戳，忽略 `NULL` 状态值，并保留上次捕获的时间戳。

**查询**：

```sql
SELECT CHANGE_CAPTURE(event_timestamp, status, 1, true) AS captured_time
FROM sensor_data;
```

**示例数据**：

| event_timestamp     | status | captured_time       |
| ------------------- | ------ | ------------------- |
| 2025-04-24 10:00:00 | 0      | NULL                |
| 2025-04-24 10:00:01 | 1      | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:02 | NULL   | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:03 | 1      | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:04 | 2      | 2025-04-24 10:00:01 |

**说明**：

- 在 `10:00:00`，`status` 为 `0`，因此未捕获值（返回 `NULL`）。
- 在 `10:00:01`，`status` 为 `1`，因此捕获 `event_timestamp`。
- 在 `10:00:02`，`status` 为 `NULL`，由于 `ignoreNull = true` 被忽略，返回上次捕获的时间戳 (`10:00:01`)。
- 在 `10:00:03`，`status` 为 `1`，但由于上一个非 `NULL` 状态为 `1`，未发生新捕获（状态未变）。
- 在 `10:00:04`，`status` 为 `2`，因此保留上次捕获的时间戳。

#### 示例 2：不忽略 NULL 值捕获测量值

**场景**：当传感器警报级别变为 `2` 时捕获温度读数，将 `NULL` 视为有效值。

**查询**：

```sql
SELECT CHANGE_CAPTURE(temperature, alert_level, 2, false) AS captured_temperature
FROM sensor_data;
```

**示例数据**：

| temperature | alert_level | captured_temperature |
| ----------- | ----------- | -------------------- |
| 25.0        | 1           | NULL                 |
| 26.5        | 2           | 26.5                 |
| 27.0        | NULL        | 26.5                 |
| 28.0        | 2           | 28.0                 |

**说明**：

- 当 `alert_level` 为 `2` 时，捕获当前的 `temperature`。
- 当 `alert_level` 为 `NULL`（且 `ignoreNull = false`）时，将其视为有效值，但由于 `NULL` ≠ `2`，返回上次捕获的 `temperature`。

## CHANGE_TO

`CHANGE_TO` 函数用于监控指定值，并在该值变为预定义目标值时返回 `true`，表示状态转换。如果监控值未变为目标值，则函数返回
`false`。可选的 `ignoreNull` 参数控制是否忽略监控字段中的 `NULL` 值，以防止其触发 `true`
结果。该函数非常适合事件驱动场景，例如需要检测特定状态变化（例如状态标志、传感器读数或模式切换）而无需捕获额外数据。

### 语法

```sql
CHANGE_TO(monitor_value, target_value [, ignoreNull])
```

#### 参数

- **monitor_value**（任意类型）：需要监控变化的字段或表达式。通常是表示状态变化的列（例如状态标志、传感器读数或模式指示器）。
- **target_value**（与 monitor_value 相同类型，可选）：触发返回 `true` 的常量值，monitor_value 必须变为该值。其数据类型必须与
  monitor_value 匹配。如果未指定，则 monitor_value 的任何变化都会触发 `true` 结果。
- **ignoreNull**（布尔值，可选）：如果为 `true`，则忽略 monitor_value 中的 `NULL` 值，函数不会对 `NULL` 转换返回 `true`。如果为
  `false` 或省略，则将 `NULL` 视为与 target_value 比较的有效值。默认值为 `false`。

#### 返回值

- 如果 monitor_value 变为 target_value（且 ignoreNull 为 `true` 时 monitor_value 不为 `NULL`），与前一个非忽略值相比，返回
  `true`。
- 如果 monitor_value 未变为 target_value，或 monitor_value 为 `NULL` 且 ignoreNull 为 `true`，返回 `false`。
- 如果没有前值（例如在流开始时），函数会评估第一个非 `NULL` 的 monitor_value（如果 ignoreNull 为 `true`）与 target_value
  的关系，以确定是否处于目标状态。

### 描述

`CHANGE_TO` 函数在流式处理环境中运行，保持状态以跟踪上一个 monitor_value 并检测到 target_value 的转换。它适用于以下场景：

- 需要检测字段的特定状态变化（例如传感器变为“开启”或状态变为“活动”）。
- 可选择忽略监控字段中的 `NULL` 值以避免错误触发。
- 不需要捕获额外数据（例如时间戳或测量值），仅需检测状态变化的发生。

常见用例包括在状态转换时触发警报、筛选满足特定条件的行，或记录系统进入特定模式的时间。

函数逐行评估流中的数据：

1. 如果 monitor_value 为 `NULL` 且 ignoreNull 为 `true`，则跳过该行并返回 `false`。
2. 如果 monitor_value 等于 target_value 且与前一个非忽略的 monitor_value 不同（且 ignoreNull 为 `true` 时 monitor_value
   不为 `NULL`），则返回 `true` 并更新存储状态。
3. 如果 monitor_value 不等于 target_value 或与前值相同，则返回 `false`。
4. 该函数是有状态的，保留上一个非忽略的 monitor_value 以与当前行进行比较。

### 示例

#### 示例 1：忽略 NULL 值检测状态变为“开启”

**场景**：在物联网系统中，检测传感器状态变为 `1`（例如“开启”），忽略 `NULL` 状态值，仅在转换时返回 `true`。

**查询**：

```sql
SELECT CHANGE_TO(status, 1, true) AS status_changed
FROM sensor_data;
```

**示例数据**：

| event_timestamp     | status | status_changed |
| ------------------- | ------ | -------------- |
| 2025-04-24 10:00:00 | 0      | false          |
| 2025-04-24 10:00:01 | 1      | true           |
| 2025-04-24 10:00:02 | NULL   | false          |
| 2025-04-24 10:00:03 | 1      | false          |
| 2025-04-24 10:00:04 | 2      | false          |

**说明**：

- 在 `10:00:00`，`status` 为 `0`，未变为 `1`，返回 `false`。
- 在 `10:00:01`，`status` 从 `0` 变为 `1`，返回 `true`。
- 在 `10:00:02`，`status` 为 `NULL`，由于 `ignoreNull = true` 被忽略，返回 `false`。
- 在 `10:00:03`，`status` 为 `1`，但之前已是 `1`（或被忽略），返回 `false`（无转换）。
- 在 `10:00:04`，`status` 为 `2`，不是 `1`，返回 `false`。

#### 示例 2：不忽略 NULL 值检测警报级别变化

**场景**：检测传感器警报级别变为 `2`，将 `NULL` 视为有效值。

**查询**：

```sql
SELECT CHANGE_TO(alert_level, 2, false) AS level_changed
FROM sensor_data;
```

**示例数据**：

| event_timestamp     | alert_level | level_changed |
| ------------------- | ----------- | ------------- |
| 2025-04-24 10:00:00 | 1           | false         |
| 2025-04-24 10:00:01 | 2           | true          |
| 2025-04-24 10:00:02 | NULL        | false         |
| 2025-04-24 10:00:03 | 2           | true          |

## CHANGE_TO

`CHANGE_TO` 函数用于监控指定值，并在该值变为预定义目标值时返回 `true`，表示状态转换。如果监控值未变为目标值，则函数返回
`false`。可选的 `ignoreNull` 参数控制是否忽略监控字段中的 `NULL` 值，以防止其触发 `true`
结果。该函数非常适合事件驱动场景，例如需要检测特定状态变化（例如状态标志、传感器读数或模式切换）而无需捕获额外数据。

### 语法

```sql
CHANGE_TO(monitor_value, target_value [, ignoreNull])
```

#### 参数

- **monitor_value**（任意类型）：需要监控变化的字段或表达式。通常是表示状态变化的列（例如状态标志、传感器读数或模式指示器）。
- **target_value**（与 monitor_value 相同类型，可选）：触发返回 `true` 的常量值，monitor_value 必须变为该值。其数据类型必须与
  monitor_value 匹配。如果未指定，则 monitor_value 的任何变化都会触发 `true` 结果。
- **ignoreNull**（布尔值，可选）：如果为 `true`，则忽略 monitor_value 中的 `NULL` 值，函数不会对 `NULL` 转换返回 `true`。如果为
  `false` 或省略，则将 `NULL` 视为与 target_value 比较的有效值。默认值为 `false`。

#### 返回值

- 如果 monitor_value 变为 target_value（且 ignoreNull 为 `true` 时 monitor_value 不为 `NULL`），与前一个非忽略值相比，返回
  `true`。
- 如果 monitor_value 未变为 target_value，或 monitor_value 为 `NULL` 且 ignoreNull 为 `true`，返回 `false`。
- 如果没有前值（例如在流开始时），函数会评估第一个非 `NULL` 的 monitor_value（如果 ignoreNull 为 `true`）与 target_value
  的关系，以确定是否处于目标状态。

### 描述

`CHANGE_TO` 函数在流式处理环境中运行，保持状态以跟踪上一个 monitor_value 并检测到 target_value 的转换。它适用于以下场景：

- 需要检测字段的特定状态变化（例如传感器变为“开启”或状态变为“活动”）。
- 可选择忽略监控字段中的 `NULL` 值以避免错误触发。
- 不需要捕获额外数据（例如时间戳或测量值），仅需检测状态变化的发生。

常见用例包括在状态转换时触发警报、筛选满足特定条件的行，或记录系统进入特定模式的时间。

函数逐行评估流中的数据：

1. 如果 monitor_value 为 `NULL` 且 ignoreNull 为 `true`，则跳过该行并返回 `false`。
2. 如果 monitor_value 等于 target_value 且与前一个非忽略的 monitor_value 不同（且 ignoreNull 为 `true` 时 monitor_value
   不为 `NULL`），则返回 `true` 并更新存储状态。
3. 如果 monitor_value 不等于 target_value 或与前值相同，则返回 `false`。
4. 该函数是有状态的，保留上一个非忽略的 monitor_value 以与当前行进行比较。

### 示例

#### 示例 1：忽略 NULL 值检测状态变为“开启”

**场景**：在物联网系统中，检测传感器状态变为 `1`（例如“开启”），忽略 `NULL` 状态值，仅在转换时返回 `true`。

**查询**：

```sql
SELECT CHANGE_TO(status, 1, true) AS status_changed
FROM sensor_data;
```

**示例数据**：

| event_timestamp     | status | status_changed |
| ------------------- | ------ | -------------- |
| 2025-04-24 10:00:00 | 0      | false          |
| 2025-04-24 10:00:01 | 1      | true           |
| 2025-04-24 10:00:02 | NULL   | false          |
| 2025-04-24 10:00:03 | 1      | false          |
| 2025-04-24 10:00:04 | 2      | false          |

**说明**：

- 在 `10:00:00`，`status` 为 `0`，未变为 `1`，返回 `false`。
- 在 `10:00:01`，`status` 从 `0` 变为 `1`，返回 `true`。
- 在 `10:00:02`，`status` 为 `NULL`，由于 `ignoreNull = true` 被忽略，返回 `false`。
- 在 `10:00:03`，`status` 为 `1`，但之前已是 `1`（或被忽略），返回 `false`（无转换）。
- 在 `10:00:04`，`status` 为 `2`，不是 `1`，返回 `false`。

#### 示例 2：不忽略 NULL 值检测警报级别变化

**场景**：检测传感器警报级别变为 `2`，将 `NULL` 视为有效值。

**查询**：

```sql
SELECT CHANGE_TO(alert_level, 2, false) AS level_changed
FROM sensor_data;
```

**示例数据**：

| event_timestamp     | alert_level | level_changed |
| ------------------- | ----------- | ------------- |
| 2025-04-24 10:00:00 | 1           | false         |
| 2025-04-24 10:00:01 | 2           | true          |
| 2025-04-24 10:00:02 | NULL        | false         |
| 2025-04-24 10:00:03 | 2           | true          |

**说明**：

- 在 `10:00:00`，`alert_level` 为 `1`，不是 `2`，返回 `false`。
- 在 `10:00:01`，`alert_level` 从 `1` 变为 `2`，返回 `true`。
- 在 `10:00:02`，`alert_level` 为 `NULL`，不是 `2`，返回 `false`。
- 在 `10:00:03`，`alert_level` 从 `NULL` 变为 `2`，返回 `true`。

## CONSECUTIVE_COUNT

`CONSECUTIVE_COUNT` 函数在流式处理环境中跟踪布尔条件连续满足的次数。它对每行评估一个布尔表达式，当条件为 `true`
时递增计数器，当条件为 `false` 时将计数器重置为 `0`。该函数非常适合需要监控条件持续性的场景，例如统计传感器读数连续满足阈值的次数或状态保持特定状态的次数。

### 语法

```sql
CONSECUTIVE_COUNT(condition_expr)
```

#### 参数

- **condition_expr**（布尔值）：对每行评估为 `true` 或 `false` 的布尔表达式。可以是比较（例如 `HvBattCooltLvl = 1`
  ）、逻辑条件组合或数据库支持的任何有效布尔表达式。

#### 返回值

- 返回一个整数，表示包括当前行在内的连续行数，其中 `condition_expr` 评估为 `true`。
- 如果当前行的 `condition_expr` 为 `false`，返回 `0`。
- 当 `condition_expr` 为 `false` 时，计数重置为 `0`，在 `condition_expr` 在 `false` 后首次为 `true` 或流开始时从 `1` 开始计数。

### 描述

`CONSECUTIVE_COUNT` 函数在流式处理环境中运行，保持状态以跟踪指定条件满足的连续行数。它适用于以下用例：

- 监控系统保持特定状态的连续次数（例如电池冷却液水平保持某个值）。
- 检测持续条件的模式，例如传感器读数连续超过阈值。
- 在条件连续满足指定行数后触发警报或操作。

函数逐行评估流中的数据：

1. 如果当前行的 `condition_expr` 为 `true`，函数递增连续 `true` 的计数（如果前一行是 `false` 或这是第一行，则从 `1` 开始）。
2. 如果 `condition_expr` 为 `false`，函数将计数重置为 `0`。
3. 该函数是有状态的，保留计数和前一行条件的状态，以确定是递增还是重置计数。

### 示例

#### 示例 1：统计连续电池冷却液水平匹配次数

**场景**：在车辆系统中，统计电池冷却液水平（`HvBattCooltLvl`）等于 `1` 的连续次数，以监控系统稳定性。

**查询**：

```sql
SELECT CONSECUTIVE_COUNT(HvBattCooltLvl = 1) AS consecutive_matches
FROM battery_data;
```

**示例数据**：

| event_timestamp     | HvBattCooltLvl | consecutive_matches |
| ------------------- | -------------- | ------------------- |
| 2025-04-24 10:00:00 | 0              | 0                   |
| 2025-04-24 10:00:01 | 1              | 1                   |
| 2025-04-24 10:00:02 | 1              | 2                   |
| 2025-04-24 10:00:03 | 1              | 3                   |
| 2025-04-24 10:00:04 | 0              | 0                   |
| 2025-04-24 10:00:05 | 1              | 1                   |

**说明**：

- 在 `10:00:00`，`HvBattCooltLvl = 0`，条件为 `false`，计数为 `0`。
- 在 `10:00:01`，`HvBattCooltLvl = 1`，条件为 `true`，计数从 `1` 开始。
- 在 `10:00:02`，`HvBattCooltLvl = 1`，条件再次为 `true`，计数递增至 `2`。
- 在 `10:00:03`，`HvBattCooltLvl = 1`，条件为 `true`，计数递增至 `3`。
- 在 `10:00:04`，`HvBattCooltLvl = 0`，条件为 `false`，计数重置为 `0`。
- 在 `10:00:05`，`HvBattCooltLvl = 1`，条件为 `true`，计数再次从 `1` 开始。

#### 示例 2：统计连续高温读数

**场景**：在传感器系统中，统计温度超过 `30` 的连续次数，以检测过热趋势。

**查询**：

```sql
SELECT CONSECUTIVE_COUNT(temperature > 30) AS consecutive_high_temp
FROM sensor_data;
```

**示例数据**：

| event_timestamp     | temperature | consecutive_high_temp |
| ------------------- | ----------- | --------------------- |
| 2025-04-24 10:00:00 | 25          | 0                     |
| 2025-04-24 10:00:01 | 32          | 1                     |
| 2025-04-24 10:00:02 | 35          | 2                     |
| 2025-04-24 10:00:03 | 28          | 0                     |

**说明**：

- 在 `10:00:00`，`temperature = 25`，条件（`temperature > 30`）为 `false`，计数为 `0`。
- 在 `10:00:01`，`temperature = 32`，条件为 `true`，计数从 `1` 开始。
- 在 `10:00:02`，`temperature = 35`，条件为 `true`，计数递增至 `2`。
- 在 `10:00:03`，`temperature = 28`，条件为 `false`，计数重置为 `0`。

## CONSECUTIVE_START

`CONSECUTIVE_START` 函数在流式处理环境中，当布尔条件在一系列连续 `true` 评估中首次满足时，捕获并返回指定的值。它从条件从
`false` 变为 `true`（或首行即为 `true`）的行中返回 `capture_value`，并在条件再次变为 `false`
前保持该值。该函数非常适合需要记录连续条件满足序列开始时的值（例如时间戳或测量值）的场景，例如标记持久状态或事件的开始。

### 语法

```sql
CONSECUTIVE_START(condition_expr, capture_value)
```

#### 参数

- **condition_expr**（布尔值）：对每行评估为 `true` 或 `false` 的布尔表达式。可以是比较（例如 `HvBattCooltLvl = 1`
  ）、逻辑条件组合或数据库支持的任何有效布尔表达式。
- **capture_value**（任意类型）：当条件首次满足时捕获其值的字段或表达式。可以是时间戳、数字、字符串或数据库支持的任何数据类型。

#### 返回值

- 返回 `condition_expr` 在连续 `true` 评估序列中首次为 `true` 的行中的 `capture_value`。
- 在后续 `condition_expr` 保持 `true` 的行中继续返回相同的 `capture_value`。
- 如果 `condition_expr` 为 `false` 或流中尚未满足 `true` 条件，则返回 `NULL`。
- 当 `condition_expr` 变为 `false` 时重置为 `NULL`，当 `condition_expr` 再次变为 `true` 时记录新的 `capture_value`。

### 描述

`CONSECUTIVE_START` 函数在流式处理环境中运行，保持状态以跟踪 `condition_expr` 为 `true` 的序列开始及其关联的
`capture_value`。它适用于以下用例：

- 捕获系统首次进入特定状态的时间戳（例如电池冷却液水平达到目标值时）。
- 记录持续条件开始时的初始测量值，例如传感器读数首次超过阈值。
- 标记事件序列的开始，例如高活动期间的开始。

函数逐行评估流中的数据：

1. 如果当前行的 `condition_expr` 为 `true`，且前一行是 `false`（或这是第一行），则捕获当前的 `capture_value` 并返回。
2. 如果 `condition_expr` 为 `true` 且前一行也是 `true`，则返回序列开始时捕获的 `capture_value`。
3. 如果 `condition_expr` 为 `false`，则返回 `NULL` 并重置状态，以便下一次 `true` 条件捕获新的 `capture_value`。
4. 该函数是有状态的，保留捕获的值和前一行条件的状态，以确定是捕获新值还是返回现有值。

### 示例

#### 示例 1：捕获连续冷却液水平匹配开始的时间戳

**场景**：在车辆系统中，捕获电池冷却液水平（`HvBattCooltLvl`）首次等于 `1` 的时间戳，以标记稳定状态的开始。

**查询**：

```sql
SELECT CONSECUTIVE_START(HvBattCooltLvl = 1, event_timestamp) AS start_time
FROM battery_data;
```

**示例数据**：

| event_timestamp     | HvBattCooltLvl | start_time          |
| ------------------- | -------------- | ------------------- |
| 2025-04-24 10:00:00 | 0              | NULL                |
| 2025-04-24 10:00:01 | 1              | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:02 | 1              | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:03 | 1              | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:04 | 0              | NULL                |
| 2025-04-24 10:00:05 | 1              | 2025-04-24 10:00:05 |

**说明**：

- 在 `10:00:00`，`HvBattCooltLvl = 0`，条件为 `false`，返回 `NULL`。
- 在 `10:00:01`，`HvBattCooltLvl = 1`，条件首次为 `true`，捕获 `event_timestamp` (`10:00:01`)。
- 在 `10:00:02` 和 `10:00:03`，`HvBattCooltLvl = 1`，条件保持 `true`，返回相同的 `start_time` (`10:00:01`)。
- 在 `10:00:04`，`HvBattCooltLvl = 0`，条件为 `false`，返回 `NULL`，状态重置。
- 在 `10:00:05`，`HvBattCooltLvl = 1`，条件再次为 `true`，捕获新的 `event_timestamp` (`10:00:05`)。

#### 示例 2：捕获高温读数开始时的温度

**场景**：在传感器系统中，捕获温度首次超过 `30` 时的温度值，以标记过热期间的开始。

**查询**：

```sql
SELECT CONSECUTIVE_START(temperature > 30, temperature) AS start_temperature
FROM sensor_data;
```

**示例数据**：

| event_timestamp     | temperature | start_temperature |
| ------------------- | ----------- | ----------------- |
| 2025-04-24 10:00:00 | 25          | NULL              |
| 2025-04-24 10:00:01 | 32          | 32                |
| 2025-04-24 10:00:02 | 35          | 32                |
| 2025-04-24 10:00:03 | 28          | NULL              |
| 2025-04-24 10:00:04 | 33          | 33                |

**说明**：

- 在 `10:00:00`，`temperature = 25`，条件（`temperature > 30`）为 `false`，返回 `NULL`。
- 在 `10:00:01`，`temperature = 32`，条件为 `true`，捕获 `temperature` (`32`)。
- 在 `10:00:02`，`temperature = 35`，条件为 `true`，返回相同的 `start_temperature` (`32`)。
- 在 `10:00:03`，`temperature = 28`，条件为 `false`，返回 `NULL`，状态重置。
- 在 `10:00:04`，`temperature = 33`，条件为 `true`，捕获新的 `temperature` (`33`)。
