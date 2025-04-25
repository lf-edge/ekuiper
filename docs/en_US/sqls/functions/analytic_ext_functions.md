# Extra Analytic Functions

## CHANGE_CAPTURE

The `CHANGE_CAPTURE` function monitors a specified value and captures (saves) another field's value when the monitored
value changes to a predefined target value. If the change condition is not met, the function returns the last captured
value. An optional `ignoreNull` parameter controls whether `NULL` values in the monitored field are ignored, preventing
them from triggering a capture. This function is ideal for event-driven scenarios where a field (e.g., timestamp,
measurement, or status) needs to be recorded upon a specific condition, with the last recorded value persisting until
the next valid trigger.

### Syntax

```sql
CHANGE_CAPTURE
(save_value, monitor_value, target_value [, ignoreNull])
```

#### Parameters

- **save_value** (any type): The field or expression whose value is captured when the condition is met. This can be a
  timestamp, numeric, string, or any supported data type in the database.
- **monitor_value** (any type): The field or expression to monitor for changes. This is typically a column indicating
  state changes (e.g., a status flag or sensor reading).
- **target_value** (same type as monitor_value, optional): The constant value that `monitor_value` must equal to trigger
  capturing `save_value`. This must match the data type of `monitor_value`. If this is not specified, any change to the
  `monitor_value` will trigger the capture.
- **ignoreNull** (boolean, optional): If `true`, `NULL` values in `monitor_value` are ignored, and the function does not
  trigger a capture or update the state. If `false` or omitted, `NULL` is treated as a valid value for comparison with
  `target_value`. Default is `false`.

#### Return Value

- Returns the most recently captured `save_value` when `monitor_value` equals `target_value` (and is not `NULL` if
  `ignoreNull` is `true`).
- If `monitor_value` does not equal `target_value` (or is `NULL` when `ignoreNull` is `true`), returns the last captured
  `save_value` from a previous trigger.
- If no value has been captured yet (e.g., at the start of the stream), returns `NULL` or a database-specific default.

### Description

The `CHANGE_CAPTURE` function operates in a streaming context, maintaining state to track the last captured value. It is
designed for scenarios where:

- A specific field needs to be recorded only when a condition (based on another field) is met.
- The last recorded value should persist until the condition is met again.
- `NULL` values in the monitored field can optionally be ignored to avoid unwanted triggers.
- Common use cases include capturing timestamps of events, recording measurements when a sensor status changes, or
  logging state transitions.

The function evaluates each row in the stream:

1. If `monitor_value` is `NULL` and `ignoreNull` is `true`, the function skips the row and returns the last captured
   `save_value`.
2. If `monitor_value` equals `target_value` (and is not `NULL` when `ignoreNull` is `true`), it captures the current
   `save_value` and updates the stored state.
3. If `monitor_value` does not equal `target_value`, it returns the previously captured `save_value` (if any).
4. The function is stateful, retaining the last captured value across rows until a new capture is triggered.

### Examples

#### Example 1: Capturing a Timestamp on Status Change with ignoreNull

**Scenario**: In an IoT system, capture the timestamp when a sensor's status becomes `1` (e.g., "on"), ignoring `NULL`
status values, and retain the last captured timestamp.

**Query**:

```sql
SELECT CHANGE_CAPTURE(event_timestamp, status, 1, true) AS captured_time
FROM sensor_data;
```

**Sample Data**:

| event_timestamp     | status | captured_time       |
|---------------------|--------|---------------------|
| 2025-04-24 10:00:00 | 0      | NULL                |
| 2025-04-24 10:00:01 | 1      | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:02 | NULL   | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:03 | 1      | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:04 | 2      | 2025-04-24 10:00:01 |

**Explanation**:

- At `10:00:00`, `status` is `0`, so no value is captured (`NULL`).
- At `10:00:01`, `status` is `1`, so `event_timestamp` is captured.
- At `10:00:02`, `status` is `NULL`, ignored due to `ignoreNull = true`, so the last captured timestamp (`10:00:01`) is
  returned.
- At `10:00:03`, `status` is `1`, but since the last non-`NULL` status was `1`, no new capture occurs (state unchanged).
- At `10:00:04`, `status` is `2`, so the last captured timestamp persists.

#### Example 2: Capturing a Measurement Without ignoreNull

**Scenario**: Capture a temperature reading when a sensor's alert level becomes `2`, treating `NULL` as a valid value.

**Query**:

```sql
SELECT CHANGE_CAPTURE(temperature, alert_level, 2, false) AS captured_temperature
FROM sensor_data;
```

**Sample Data**:

| temperature | alert_level | captured_temperature |
|-------------|-------------|----------------------|
| 25.0        | 1           | NULL                 |
| 26.5        | 2           | 26.5                 |
| 27.0        | NULL        | 26.5                 |
| 28.0        | 2           | 28.0                 |

**Explanation**:

- When `alert_level` is `2`, the current `temperature` is captured.
- When `alert_level` is `NULL` (with `ignoreNull = false`), it’s treated as a valid value, but since `NULL` ≠ `2`, the
  last captured `temperature` is returned.

## CHANGE_TO

The `CHANGE_TO` function monitors a specified value and returns `true` when the monitored value changes to a predefined
target value, indicating a state transition. If the monitored value does not change to the target value, the function
returns `false`. An optional `ignoreNull` parameter controls whether `NULL` values in the monitored field are ignored,
preventing them from triggering a `true` result. This function is ideal for event-driven scenarios where detecting a
specific state change (e.g., a status flag, sensor reading, or mode switch) is needed without capturing additional data.

### Syntax

```sql
CHANGE_TO
(monitor_value, target_value [, ignoreNull])
```

#### Parameters

- **monitor_value** (any type): The field or expression to monitor for changes. This is typically a column indicating
  state changes (e.g., a status flag, sensor reading, or mode indicator).
- **target_value** (same type as monitor_value, optional): The constant value that `monitor_value` must change to in
  order to return `true`. This must match the data type of `monitor_value`. If not specified, any change to
  `monitor_value` triggers a `true` result.
- **ignoreNull** (boolean, optional): If `true`, `NULL` values in `monitor_value` are ignored, and the function does not
  return `true` for `NULL` transitions. If `false` or omitted, `NULL` is treated as a valid value for comparison with
  `target_value`. Default is `false`.

#### Return Value

- Returns `true` if `monitor_value` changes to `target_value` (and is not `NULL` if `ignoreNull` is `true`) compared to
  the previous non-ignored value.
- Returns `false` if `monitor_value` does not change to `target_value`, or if `monitor_value` is `NULL` when
  `ignoreNull` is `true`.
- If no previous value exists (e.g., at the start of the stream), the function evaluates the first non-`NULL`
  `monitor_value` (if `ignoreNull` is `true`) against `target_value` to determine if it starts in the target state.

### Description

The `CHANGE_TO` function operates in a streaming context, maintaining state to track the previous `monitor_value` and
detect transitions to the `target_value`. It is designed for scenarios where:

- A specific state change in a field needs to be detected (e.g., a sensor turning "on" or a status becoming "active").
- `NULL` values in the monitored field can optionally be ignored to avoid false triggers.
- No additional data (e.g., timestamps or measurements) needs to be captured, only the occurrence of the state change is
  required.

Common use cases include triggering alerts on state transitions, filtering rows where a specific condition is met, or
logging when a system enters a particular mode.

The function evaluates each row in the stream:

1. If `monitor_value` is `NULL` and `ignoreNull` is `true`, the function skips the row and returns `false`.
2. If `monitor_value` equals `target_value` and differs from the previous non-ignored `monitor_value` (and is not `NULL`
   when `ignoreNull` is `true`), it returns `true` and updates the stored state.
3. If `monitor_value` does not equal `target_value` or is the same as the previous value, it returns `false`.
4. The function is stateful, retaining the last non-ignored `monitor_value` to compare against the current row.

### Examples

#### Example 1: Detecting Status Change to "On" with ignoreNull

**Scenario**: In an IoT system, detect when a sensor's status changes to `1` (e.g., "on"), ignoring `NULL` status
values, and return `true` only for the transition.

**Query**:

```sql
SELECT CHANGE_TO(status, 1, true) AS status_changed
FROM sensor_data;
```

**Sample Data**:

| event_timestamp     | status | status_changed |
|---------------------|--------|----------------|
| 2025-04-24 10:00:00 | 0      | false          |
| 2025-04-24 10:00:01 | 1      | true           |
| 2025-04-24 10:00:02 | NULL   | false          |
| 2025-04-24 10:00:03 | 1      | false          |
| 2025-04-24 10:00:04 | 2      | false          |

**Explanation**:

- At `10:00:00`, `status` is `0`, so no change to `1` (`false`).
- At `10:00:01`, `status` changes to `1` from `0`, so `true` is returned.
- At `10:00:02`, `status` is `NULL`, ignored due to `ignoreNull = true`, so `false` is returned.
- At `10:00:03`, `status` is `1`, but it was already `1` (or ignored), so `false` (no transition).
- At `10:00:04`, `status` is `2`, not `1`, so `false`.

#### Example 2: Detecting Alert Level Change Without ignoreNull

**Scenario**: Detect when a sensor's alert level changes to `2`, treating `NULL` as a valid value.

**Query**:

```sql
SELECT CHANGE_TO(alert_level, 2, false) AS level_changed
FROM sensor_data;
```

**Sample Data**:

| event_timestamp     | alert_level | level_changed |
|---------------------|-------------|---------------|
| 2025-04-24 10:00:00 | 1           | false         |
| 2025-04-24 10:00:01 | 2           | true          |
| 2025-04-24 10:00:02 | NULL        | false         |
| 2025-04-24 10:00:03 | 2           | true          |

**Explanation**:

- At `10:00:00`, `alert_level` is `1`, not `2`, so `false`.
- At `10:00:01`, `alert_level` changes to `2` from `1`, so `true`.
- At `10:00:02`, `alert_level` is `NULL`, which is not `2`, so `false`.
- At `10:00:03`, `alert_level` changes to `2` from `NULL`, so `true`.

## CONSECUTIVE_COUNT

The `CONSECUTIVE_COUNT` function tracks the number of consecutive times a boolean condition is met in a streaming
context. It evaluates a boolean expression for each row and increments a counter when the condition is `true`, resetting
the counter to `0` when the condition is `false`. This function is ideal for scenarios where you need to monitor the
persistence of a condition, such as counting how many consecutive times a sensor reading meets a threshold or a status
remains in a specific state.

### Syntax

```sql
CONSECUTIVE_COUNT
(condition_expr)
```

#### Parameters

- **condition_expr** (boolean): A boolean expression that evaluates to `true` or `false` for each row. This can be a
  comparison (e.g., `HvBattCooltLvl = 1`), a logical combination of conditions, or any valid boolean expression
  supported by the database.

#### Return Value

- Returns an integer representing the number of consecutive rows (including the current row) where `condition_expr`
  evaluates to `true`.
- Returns `0` if `condition_expr` is `false` for the current row.
- The count resets to `0` when `condition_expr` is `false` and starts at `1` the first time `condition_expr` is `true`
  after a `false` or at the start of the stream.

### Description

The `CONSECUTIVE_COUNT` function operates in a streaming context, maintaining state to track the number of consecutive
rows where the specified condition is met. It is designed for use cases such as:

- Monitoring how many consecutive times a system remains in a specific state (e.g., a battery coolant level at a certain
  value).
- Detecting patterns of persistent conditions, such as consecutive sensor readings above a threshold.
- Triggering alerts or actions after a condition has been met for a specified number of consecutive rows.

The function evaluates each row in the stream:

1. If `condition_expr` is `true` for the current row, the function increments the count of consecutive `true`
   occurrences (starting at `1` if the previous row was `false` or it’s the first row).
2. If `condition_expr` is `false`, the function resets the count to `0`.
3. The function is stateful, retaining the count and the state of the previous row’s condition to determine whether to
   increment or reset the count.

### Examples

#### Example 1: Counting Consecutive Battery Coolant Level Matches

**Scenario**: In a vehicle system, count the number of consecutive times the battery coolant level (`HvBattCooltLvl`)
equals `1` to monitor system stability.

**Query**:

```sql
SELECT CONSECUTIVE_COUNT(HvBattCooltLvl = 1) AS consecutive_matches
FROM battery_data;
```

**Sample Data**:

| event_timestamp     | HvBattCooltLvl | consecutive_matches |
|---------------------|----------------|---------------------|
| 2025-04-24 10:00:00 | 0              | 0                   |
| 2025-04-24 10:00:01 | 1              | 1                   |
| 2025-04-24 10:00:02 | 1              | 2                   |
| 2025-04-24 10:00:03 | 1              | 3                   |
| 2025-04-24 10:00:04 | 0              | 0                   |
| 2025-04-24 10:00:05 | 1              | 1                   |

**Explanation**:

- At `_10:00:00`, `HvBattCooltLvl = 0`, condition is `false`, so count is `0`.
- At `10:00:01`, `HvBattCooltLvl = 1`, condition is `true`, so count starts at `1`.
- At `10:00:02`, `HvBattCooltLvl = 1`, condition is `true` again, so count increments to `2`.
- At `10:00:03`, `HvBattCooltLvl = 1`, condition is `true`, so count increments to `3`.
- At `10:00:04`, `HvBattCooltLvl = 0`, condition is `false`, so count resets to `0`.
- At `10:00:05`, `HvBattCooltLvl = 1`, condition is `true`, so count starts again at `1`.

#### Example 2: Counting Consecutive High Temperature Readings

**Scenario**: In a sensor system, count the number of consecutive times the temperature exceeds `30` to detect
overheating trends.

**Query**:

```sql
SELECT CONSECUTIVE_COUNT(temperature > 30) AS consecutive_high_temp
FROM sensor_data;
```

**Sample Data**:

| event_timestamp     | temperature | consecutive_high_temp |
|---------------------|-------------|-----------------------|
| 2025-04-24 10:00:00 | 25          | 0                     |
| 2025-04-24 10:00:01 | 32          | 1                     |
| 2025-04-24 10:00:02 | 35          | 2                     |
| 2025-04-24 10:00:03 | 28          | 0                     |

**Explanation**:

- At `10:00:00`, `temperature = 25`, condition (`temperature > 30`) is `false`, so count is `0`.
- At `10:00:01`, `temperature = 32`, condition is `true`, so count starts at `1`.
- At `10:00:02`, `temperature = 35`, condition is `true`, so count increments to `2`.
- At `10:00:03`, `temperature = 28`, condition is `false`, so count resets to `0`.

## CONSECUTIVE_START

The `CONSECUTIVE_START` function captures and returns a specified value when a boolean condition is first met in a
sequence of consecutive `true` evaluations in a streaming context. It returns the `capture_value` from the row where the
condition transitions from `false` to `true` (or is `true` for the first row) and retains that value until the condition
becomes `false` again. This function is ideal for scenarios where you need to record a value (e.g., a timestamp or
measurement) at the start of a consecutive sequence of a condition being met, such as the beginning of a persistent
state or event.

### Syntax

```sql
CONSECUTIVE_START
(condition_expr, capture_value)
```

#### Parameters

- **condition_expr** (boolean): A boolean expression that evaluates to `true` or `false` for each row. This can be a
  comparison (e.g., `HvBattCooltLvl = 1`), a logical combination of conditions, or any valid boolean expression
  supported by the database.
- **capture_value** (any type): The field or expression whose value is captured when the condition is first met in a
  consecutive sequence. This can be a timestamp, numeric, string, or any supported data type in the database.

#### Return Value

- Returns the `capture_value` from the row where `condition_expr` first evaluates to `true` in a sequence of consecutive
  `true` evaluations.
- Continues to return the same `capture_value` for subsequent rows where `condition_expr` remains `true`.
- Returns `NULL` if `condition_expr` is `false` or if no `true` condition has been met yet in the stream.
- Resets to `NULL` when `condition_expr` becomes `false`, and a new `capture_value` is recorded when `condition_expr`
  becomes `true` again.

### Description

The `CONSECUTIVE_START` function operates in a streaming context, maintaining state to track the start of a sequence
where `condition_expr` is `true` and the associated `capture_value`. It is designed for use cases such as:

- Capturing the timestamp when a system first enters a specific state (e.g., when a battery coolant level reaches a
  target value).
- Recording the initial measurement at the start of a persistent condition, such as the first sensor reading above a
  threshold.
- Marking the beginning of an event sequence, such as the start of a period of high activity.

The function evaluates each row in the stream:

1. If `condition_expr` is `true` for the current row and the previous row was `false` (or it’s the first row), it
   captures the current `capture_value` and returns it.
2. If `condition_expr` is `true` and the previous row was also `true`, it returns the previously captured
   `capture_value` from the start of the sequence.
3. If `condition_expr` is `false`, it returns `NULL` and resets the state, so the next `true` condition will capture a
   new `capture_value`.
4. The function is stateful, retaining the captured value and the state of the previous row’s condition to determine
   whether to capture a new value or return the existing one.

### Examples

#### Example 1: Capturing Timestamp at Start of Consecutive Coolant Level Match

**Scenario**: In a vehicle system, capture the timestamp when the battery coolant level (`HvBattCooltLvl`) first equals
`1` at the start of a consecutive sequence to mark the beginning of a stable state.

**Query**:

```sql
SELECT CONSECUTIVE_START(HvBattCooltLvl = 1, event_timestamp) AS start_time
FROM battery_data;
```

**Sample Data**:

| event_timestamp     | HvBattCooltLvl | start_time          |
|---------------------|----------------|---------------------|
| 2025-04-24 10:00:00 | 0              | NULL                |
| 2025-04-24 10:00:01 | 1              | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:02 | 1              | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:03 | 1              | 2025-04-24 10:00:01 |
| 2025-04-24 10:00:04 | 0              | NULL                |
| 2025-04-24 10:00:05 | 1              | 2025-04-24 10:00:05 |

**Explanation**:

- At `10:00:00`, `HvBattCooltLvl = 0`, condition is `false`, so `NULL` is returned.
- At `10:00:01`, `HvBattCooltLvl = 1`, condition is `true` for the first time, so `event_timestamp` (`10:00:01`) is
  captured.
- At `10:00:02` and `10:00:03`, `HvBattCooltLvl = 1`, condition remains `true`, so the same `start_time` (`10:00:01`) is
  returned.
- At `10:00:04`, `HvBattCooltLvl = 0`, condition is `false`, so `NULL` is returned, and the state resets.
- At `10:00:05`, `HvBattCooltLvl = 1`, condition is `true` again, so a new `event_timestamp` (`10:00:05`) is captured.

#### Example 2: Capturing Temperature at Start of High Readings

**Scenario**: In a sensor system, capture the temperature when it first exceeds `30` at the start of a consecutive
sequence to mark the beginning of an overheating period.

**Query**:

```sql
SELECT CONSECUTIVE_START(temperature > 30, temperature) AS start_temperature
FROM sensor_data;
```

**Sample Data**:

| event_timestamp     | temperature | start_temperature |
|---------------------|-------------|-------------------|
| 2025-04-24 10:00:00 | 25          | NULL              |
| 2025-04-24 10:00:01 | 32          | 32                |
| 2025-04-24 10:00:02 | 35          | 32                |
| 2025-04-24 10:00:03 | 28          | NULL              |
| 2025-04-24 10:00:04 | 33          | 33                |

**Explanation**:

- At `10:00:00`, `temperature = 25`, condition (`temperature > 30`) is `false`, so `NULL` is returned.
- At `10:00:01`, `temperature = 32`, condition is `true`, so `temperature` (`32`) is captured.
- At `10:00:02`, `temperature = 35`, condition is `true`, so the same `start_temperature` (`32`) is returned.
- At `10:00:03`, `temperature = 28`, condition is `false`, so `NULL` is returned, and the state resets.
- At `10:00:04`, `temperature = 33`, condition is `true`, so a new `temperature` (`33`) is captured.
