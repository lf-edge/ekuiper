# Functions

eKuiper has many built-in functions for performing calculations on data.

## Aggregate Functions
Aggregate functions perform a calculation on a set of values and return a single value. Aggregate functions can be used as expressions only in the following:
* The select list of a SELECT statement (either a sub-query or an outer query).
* A HAVING clause.

| Function    | Example                   | Description                                                                                                                                                                                                                                                                                                                                                                                                             |
|-------------|---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| avg         | avg(col1)                 | The average of the values in a group. The null values will be ignored.                                                                                                                                                                                                                                                                                                                                                  |
| count       | count(*)                  | The number of items in a group. The null values will be ignored.                                                                                                                                                                                                                                                                                                                                                        |
| max         | max(col1)                 | The maximum value in a group. The null values will be ignored.                                                                                                                                                                                                                                                                                                                                                          |
| min         | min(col1)                 | The minimum value in a group. The null values will be ignored.                                                                                                                                                                                                                                                                                                                                                          |
| sum         | sum(col1)                 | The sum of all the values in a group. The null values will be ignored.                                                                                                                                                                                                                                                                                                                                                  |
| collect     | collect(*), collect(col1) | Returns an array with all column or the whole record (when the parameter is *) values from the group.                                                                                                                                                                                                                                                                                                                   |
| deduplicate | deduplicate(col, false)   | Returns the deduplicate results in the group, usually a window. The first argument is the column as the key to deduplicate; the second argument is whether to return all items or just the latest item which is not duplicate. If the latest item is a duplicate, the sink will receive an empty map. Set the sink property [omitIfEmpty](../rules/overview.md#Sinks/Actions) to the sink to not triggering the action. |

### Collect() Examples

- Get an array of column `a` of the current window. Assume the column `a` is of int type, the result will be like: `[{"r1":[32, 45]}]`
    ```sql
    SELECT collect(a) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```
- Get the whole array of the current window. The result will be like: `[{"r1":{"a":32, "b":"hello"}, {"a":45, "b":"world"}}]`
    ```sql
    SELECT collect(*) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```
  
- Get the second element's column 'a' value within the current window. The result will be like: `[{"r1":32}]`
    ```sql
    SELECT collect(*)[1]->a as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

### Deduplicate() Examples

 - Get the whole array of the current window which is deduplicated by column `a`. The result will be like: `[{"r1":{"a":32, "b":"hello"}, {"a":45, "b":"world"}}]`
     ```sql
     SELECT deduplicate(a, true) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
     ```
 - Get the column `a` value which is not duplicate during the last hour. The result will be like: `[{"r1":32}]`, `[{"r1":45}]` and `[{}]` if a duplicate value arrives. Use the omitIfEmpty sink property to filter out those empty results.
      ```sql
      SELECT deduplicate(a, false)->a as r1 FROM demo GROUP BY SlidingWindow(hh, 1)
      ```


## Mathematical Functions
| Function | Example            | Description                                                                                                                                                                                     |
|----------|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| abs      | abs(col1)          | The absolute value of a value                                                                                                                                                                   |
| acos     | acos(col1)         | The inverse cosine of a number in radians                                                                                                                                                       |
| asin     | asin(col1)         | The inverse sine of a number in radians                                                                                                                                                         |
| atan     | atan(col1)         | The inverse tangent of a number in radians                                                                                                                                                      |
| atan2    | atan2(col1, col2)  | The angle, in radians,  between the positive x-axis and the (x, y) point defined in the two arguments                                                                                           |
| bitand   | bitand(col1, col2) | Performs a bitwise AND on the bit representations of the two Int(-converted) arguments                                                                                                          |
| bitor    | bitor(col1, col2)  | Performs a bitwise OR of the bit representations of the two arguments                                                                                                                           |
| bitxor   | bitxor(col1, col2) | Performs a bitwise XOR on the bit representations of the two Int(-converted) arguments                                                                                                          |
| bitnot   | bitnot(col1)       | Performs a bitwise NOT on the bit representations of the Int(-converted) argument                                                                                                               |
| ceil     | ceil(col1)         | Round a value up to the nearest BIGINT value.                                                                                                                                                   |
| cos      | cos(col1)          | Returns the cosine of a number in radians.                                                                                                                                                      |
| cosh     | cosh(col1)         | Returns the hyperbolic cosine of a number in radians.                                                                                                                                           |
| exp      | exp(col1)          | Returns e raised to the Decimal argument.                                                                                                                                                       |
| ln       | ln(col1)           | Returns the natural logarithm of the argument.                                                                                                                                                  |
| log      | log(col1)          | Returns the base 10 logarithm of the argument.                                                                                                                                                  |
| mod      | mod(col1, col2)    | Returns the remainder of the division of the first argument by the second argument.                                                                                                             |
| power    | power(x, y)        | Pow returns x**y, the base-x exponential of y.                                                                                                                                                  |
| rand     | rand()             | Returns a pseudorandom, uniformly distributed double between 0.0 and 1.0.                                                                                                                       |
| round    | round(col1)        | Round a value to the nearest BIGINT value.                                                                                                                                                      |
| sign     | sign(col1)         | Returns the sign of the given number. When the sign of the argument is positive, 1 is returned. When the sign of the argument is negative, -1 is returned. If the argument is 0, 0 is returned. |
| sin      | sin(col1)          | Returns the sine of a number in radians.                                                                                                                                                        |
| sinh     | sinh(col1)         | Returns the hyperbolic sine of a number in radians.                                                                                                                                             |
| sqrt     | sqrt(col1)         | Returns the square root of a number.                                                                                                                                                            |
| tan      | tan(col1)          | Returns the tangent of a number in radians.                                                                                                                                                     |
| tanh     | tanh(col1)         | Returns the hyperbolic tangent of a number in radians.                                                                                                                                          |

## String Functions

| Function       | Example                                | Description                                                                                                                                                                                                                                                                                                                                      |
|----------------|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| concat         | concat(col1...)                        | Concatenates arrays or strings. This function accepts any number of arguments and returns a String or an Array                                                                                                                                                                                                                                   |
| endswith       | endswith(col1, col2)                   | Returns a Boolean indicating whether the first String argument ends with the second String argument.                                                                                                                                                                                                                                             |
| format_time    | format_time(col1, format)              | Format a datetime to string. The 'col1' will be [casted to datetime type](#cast-to-datetime) if it is bigint, float or string type before formatting. Please check [format patterns](#format_time-patterns) for how to compose the format.                                                                                                       |
| indexof        | indexof(col1, col2)                    | Returns the first index (0-based) of the second argument as a substring in the first argument.                                                                                                                                                                                                                                                   |
| length         | length(col1)                           | Returns the number of characters in the provided string.                                                                                                                                                                                                                                                                                         |
| lower          | lower(col1)                            | Returns the lowercase version of the given String.                                                                                                                                                                                                                                                                                               |
| lpad           | lpad(col1, 2)                          | Returns the String argument, padded on the left side with the number of spaces specified by the second argument.                                                                                                                                                                                                                                 |
| ltrim          | ltrim(col1)                            | Removes all leading whitespace (tabs and spaces) from the provided String.                                                                                                                                                                                                                                                                       |
| numbytes       | numbytes(col1)                         | Returns the number of bytes in the UTF-8 encoding of the provided string.                                                                                                                                                                                                                                                                        |
| regexp_matches | regexp_matches(col1, regex)            | Returns true if the string (first argument) contains a match for the regular expression.                                                                                                                                                                                                                                                         |
| regexp_replace | regexp_replace(col1, regex, str)       | Replaces all occurrences of the second argument (regular expression) in the first argument with the third argument.                                                                                                                                                                                                                              |
| regexp_substr  | regexp_substr(col1, regex)             | Finds the first match of the 2nd parameter (regex) in the first parameter.                                                                                                                                                                                                                                                                       |
| rpad           | rpad(col1, 2)                          | Returns the String argument, padded on the right side with the number of spaces specified by the second argument.                                                                                                                                                                                                                                |
| rtrim          | rtrim(col1)                            | Removes all trailing whitespace (tabs and spaces) from the provided String.                                                                                                                                                                                                                                                                      |
| substring      | substring(col1, start, end)            | returns the substring of the provided String from the provided Int index (0-based, inclusive) to the end of the String.                                                                                                                                                                                                                          |
| startswith     | startswith(col1, str)                  | Returns Boolean, whether the first string argument starts with the second string argument.                                                                                                                                                                                                                                                       |
| split_value    | split_value(col1, str_splitter, index) | Split the value of the 1st parameter with the 2nd parameter, and return the value of split array that indexed with the 3rd parameter.<br />`split_value("/test/device001/message","/",0) AS a`, the returned value of function is empty; <br />`split_value("/test/device001/message","/",3) AS a`, the returned value of function is `message`; |
| trim           | trim(col1)                             | Removes all leading and trailing whitespace (tabs and spaces) from the provided String.                                                                                                                                                                                                                                                          |
| upper          | upper(col1)                            | Returns the uppercase version of the given String.                                                                                                                                                                                                                                                                                               |

### Format_time patterns

A pattern is used to create a format string. Patterns are based on a simple sequence of letters and symbols which is common in many languages like Java etc. The supported symbols in Kuiper are

| Symbol | Meaning                   | Example                               |
|--------|---------------------------|---------------------------------------|
| G      | era                       | G(AD)                                 |
| Y      | year                      | YYYY(2004), YY(04)                    |
| M      | month                     | M(1), MM(01), MMM(Jan), MMMM(January) |
| d      | day of month              | d(2), dd(02)                          |
| E      | day of week               | EEE(Mon), EEEE(Monday)                |
| H      | hour in 24 hours format   | HH(15)                                |
| h      | hour in 12 hours format   | h(2), hh(03)                          |
| a      | AM or PM                  | a(PM)                                 |
| m      | minute                    | m(4), mm(04)                          |
| s      | second                    | s(5), ss(05)                          |
| S      | fraction of second        | S(.0), SS(.00), SSS(.000)             |
| z      | time zone name            | z(MST)                                |
| Z      | 4 digits time zone offset | Z(-0700)                              |
| X      | time zone offset          | X(-07), XX(-0700), XXX(-07:00)        |

Examples:

- YYYY-MM-dd T HH:mm:ss -> 2006-01-02 T 15:04:05
- YYYY/MM/dd HH:mm:ssSSS XXX -> 2006/01/02 15:04:05.000 -07:00
 
## Conversion Functions

| Function         | Example                          | Description                                                                                                                                                                                                                                                                                       |
|------------------|----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cast             | cast(col,  "bigint")             | Converts a value from one data type to another. The supported types includes: bigint, float, string, boolean and datetime.                                                                                                                                                                        |
| chr              | chr(col1)                        | Returns the ASCII character that corresponds to the given Int argument                                                                                                                                                                                                                            |
| encode           | encode(col1, "base64")           | Use the encode function to encode the payload, which potentially might be non-JSON data, into its string representation based on the encoding scheme. Currently, only "base64" encoding type is supported.                                                                                        |
| trunc            | trunc(dec, int)                  | Truncates the first argument to the number of Decimal places specified by the second argument. If the second argument is less than zero, it is set to zero. If the second argument is greater than 34, it is set to 34. Trailing zeroes are stripped from the result.                             |
| object_construct | object_construct(key1, col, ...) | Return a struct type object/map constructed by the arguments. The arguments are series of key value pairs, thus the arguments count must be an odd number. The key must a string and the value can be of any type. If the value is null, the key/value pair will not present in the final object. |

### Cast to datetime

When casting to datetime type, the supported column type and casting rule are:

1. If column is datetime type, just return the value.
2. If column is bigint or float type, the number will be treated as the milliseconds elapsed since January 1, 1970 00:00:00 UTC and converted.
3. If column is string, it will be parsed to datetime with the default format: `"2006-01-02T15:04:05.000Z07:00"`.
4. Other types are not supported.

## Hashing Functions
| Function | Example      | Description                  |
|----------|--------------|------------------------------|
| md5      | md5(col1)    | Hashed value of the argument |
| sha1     | sha1(col1)   | Hashed value of the argument |
| sha256   | sha256(col1) | Hashed value of the argument |
| sha384   | sha384(col1) | Hashed value of the argument |
| sha512   | sha512(col1) | Hashed value of the argument |
## JSON Functions
| Function              | Example                               | Description                                                                                |
|-----------------------|---------------------------------------|--------------------------------------------------------------------------------------------|
| json_path_exists      | json_path_exists(col1, "$.name")      | Checks whether JSON path returns any item for the specified JSON value. Return bool value. |
| json_path_query       | json_path_query(col1, "$.name")       | Gets all items returned by JSON path for the specified JSON value.                         |
| json_path_query_first | json_path_query_first(col1, "$.name") | Gets the first item returned by JSON path for the specified JSON value.                    |

**Please refer to [json path functions](./json_expr.md#json-path-functions) for how to compose a json path.**  

## Other Functions
| Function     | Example                | Description                                                                                                                                                                                                                                                                                                                                                                                       |
|--------------|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| isNull       | isNull(col1)           | Returns true if the argument is the Null value.                                                                                                                                                                                                                                                                                                                                                   |
| cardinality  | cardinality(col1)      | The number of members in the group. The null value is 0.                                                                                                                                                                                                                                                                                                                                          |
| newuuid      | newuuid()              | Returns a random 16-byte UUID.                                                                                                                                                                                                                                                                                                                                                                    |
| tstamp       | tstamp()               | Returns the current timestamp in milliseconds from 00:00:00 Coordinated Universal Time (UTC), Thursday, 1 January 1970                                                                                                                                                                                                                                                                            |
| mqtt         | mqtt(topic)            | Returns the MQTT meta-data of specified key. The current supported keys<br />- topic: return the topic of message.  If there are multiple stream source, then specify the source name in parameter. Such as `mqtt(src1.topic)`<br />- messageid: return the message id of message. If there are multiple stream source, then specify the source name in parameter. Such as `mqtt(src2.messageid)` |
| meta         | meta(topic)            | Returns the meta-data of specified key. The key could be:<br/> - a standalone key if there is only one source in the from clause, such as `meta(device)`<br />- A qualified key to specify the stream, such as `meta(src1.device)` <br />- A key with arrow for multi level meta data, such as `meta(src1.reading->device->name)` This assumes reading is a map structure meta data.              |
| window_start | window_start()         | Return the window start timestamp in int64 format. If there is no time window, it returns 0. The window time is aligned with the timestamp notion of the rule. If the rule is using processing time, then the window start timestamp is the processing timestamp. If the rule is using event time, then the window start timestamp is the event timestamp.                                        |
| window_end   | window_end()           | Return the window end timestamp in int64 format. If there is no time window, it returns 0. The window time is aligned with the timestamp notion of the rule. If the rule is using processing time, then the window start timestamp is the processing timestamp. If the rule is using event time, then the window start timestamp is the event timestamp.                                          |
| changed_col  | changed_col(true, col) | Return the column value if it has changed from the last execution.                                                                                                                                                                                                                                                                                                                                |

## Multiple Column Functions

A multiple column function is a function that returns multiple columns. Contrast to normal scalar function, which returns a single column of a single row.

Multiple column function can only be used in the `SELECT` clause of a query. 

| Function     | Example                                      | Description                                                                                                  |
|--------------|----------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| changed_cols | changed_cols(prefix, ignoreNull, colA, colB) | Return the changed columns whose name are prefixed. Check [changed_cols](#changed_cols-function) for detail. |

## Functions to detect changes

### Changed_col function

This function is a normal scalar function, so it can be used in any clause including SELECT and WHERE. 

**Syntax**

```CHANGED_COL(<ignoreNull>, <expr>)```

**Arguments**

**ignoreNull**: whether to ignore null values when comparing for changes. If true, the null value won’t emit a change.

**expr**: An expression to be selected and monitored for the changed status.

**Returns**

Return the changed value or nil with column name changed_col by default like any other functions. Use `as alias` to rename the column.

### Changed_cols function

This function returns multiple columns, so it is only allowed in the SELECT clause.

**Syntax**

```CHANGED_COLS (<prefix>, <ignoreNull>, <expr> [,...,<exprN>])```

**Arguments**

**prefix**: The prefix of the selected column name. By default, the selected name will be the same as select the expr directly. For example, `CHANGED_COLS("", true, col1)` will return `col1` as the name. If setting a prefix, the return name will have that prefix. For example, `CHANGED_COLS("changed_", true, col1)` will return `changed_col1` as the name.

**ignoreNull**: whether to ignore null values when detecting changes. If true, the null value won’t trigger a change.

**expr**: An expression to be selected and monitored for the changed status. Allow any expression that can be used in select clause. The expression can be a `*` which will return multiple columns by one expression.

**Returns**

Return all changed values compared to the previous sink result. So if using in a scalar rule, it will compare to the previous value emit. If using in a window, it will compare to the previous window result.

In the first run, all expressions will be returned because there is no previous result.

In the consequent runs, if nothing changed, it can emit nothing. And if the sink has the default omitEmpty, the sink will not be triggerred.

**Notice**

The multiple column outputs can only be used in the select clause. Even the selected result cannot be access in WHERE or other place. If filter based on the value is needed, use CHANGED_COL or set the result of multiple column outputs as the prior rule in a rule chain.

For multiple column outputs, the alias can only be set  generally with the prefix. To set alias for each column separately, try to call the changed function for each column respectively and use as to set alias. 

### Examples

Create a stream demo and have below inputs

```json lines
{"ts":1, "temperature":23, "humidity":88}
{"ts":2, "temperature":23, "humidity":88}
{"ts":3, "temperature":23, "humidity":88}
{"ts":4, "temperature":25, "humidity":88}
{"ts":5, "temperature":25, "humidity":90}
{"ts":6, "temperature":25, "humidity":91}
{"ts":7, "temperature":25, "humidity":91}
{"ts":8, "temperature":25, "humidity":91}
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
{"c_temperature":23,"c_humidity":88}
{"c_temperature":25}
{"c_humidity":90}
{"c_humidity":91}
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
SQL: SELECT id, temperature, humidity FROM demo
WHERE ISNULL(CHANGED_COL(true, temperature)) = false OR ISNULL(CHANGED_COL(humidity)) = false
_________________________________________________________
{"ts":1,temperature":23,"humidity":88}
{"ts":4,temperature":25,"humidity":88}
{"ts":5,temperature":25,"humidity":90}
{"ts":6,temperature":25,"humidity":91}
```

Rule to get the events when temperature has changed but humidity has NOT changed:

```text
SQL: SELECT id, temperature, humidity FROM demo 
WHERE ISNULL(CHANGED_COL(true, temperature)) = false AND ISNULL(CHANGED_COL(true, humidity))
_________________________________________________________
{"ts":1,temperature":23,"humidity":88}
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
SQL: SELECT id, temperature, humidity FROM demo 
WHERE CHANGED_COL(true, temperature) > 24
_________________________________________________________
{"ts":4,temperature":25,"humidity":88}
```