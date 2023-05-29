# Data types

In eKuiper, each column or an expression has a related data type. A data type describes (and constrains) the set of values that a column of that type can hold or an expression of that type can produce.



## Supported data types

Below is the list of data types supported.

| # | Data type | Description                                                                                                                |
|---|-----------|----------------------------------------------------------------------------------------------------------------------------|
| 1 | bigint    | The int type.                                                                                                              |
| 2 | float     | The float type.                                                                                                            |
| 3 | string    | Text values, comprised of Unicode characters.                                                                              |
| 4 | datetime  | datetime type.                                                                                                             |
| 5 | boolean   | The boolean type, the value could be `true` or `false`.                                                                    |
| 6 | bytea     | A sequence of bytes to store binary data. If the stream format is "JSON", the bytea field must be a base64 encoded string. |
| 7 | array     | The array type, can be any types from simple data or struct type.                                                          |
| 8 | struct    | The complex type. Set of name/value pairs. Values must be of supported data type.                                          |

## Compatibility of comparison and calculation

There may be binary operations in each sql clause. In this
example, `Select temperature * 2 from demo where temperature > 20`, a calculation operation is used in select clause and
a comparison operation is used in the where clause. In the binary operations, if incompatible data types are used, a
runtime error will happen and send to the sinks.

Array and struct are not supported in any binary operations. The compatibility of other data types is listed in below
table. Whereas, the row header is the left operand data type and the column header is the right operand data. The value
is the compatibility in which Y stands for yes and N stands for no.

| #        | bigint | float | string                    | datetime | boolean |
|----------|--------|-------|---------------------------|----------|---------|
| bigint   | Y      | Y     | N                         | N        | N       |
| float    | Y      | Y     | N                         | N        | N       |
| string   | N      | N     | Y                         | N        | N       |
| datetime | Y      | Y     | Y, if in the valid format | Y        | N       |
| boolean  | N      | N     | N                         | N        | Y       |

 The default format for datetime string is `"2006-01-02T15:04:05.000Z07:00"`

 For `nil` value, we follow the rules:

  1. Compare with nil always return false
  2. Calculate with nil always return nil

## Type conversions

There is a built-in function `cast(col, targetType)` to explicitly convert from one date type to another in runtime.
Please refer to [cast](./functions/transform_functions.md) for detail.
