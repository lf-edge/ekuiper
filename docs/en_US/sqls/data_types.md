# Data types

In Kuiper, each column or an expression has a related data type. A data type describes (and constrains) the set of values that a column of that type can hold or an expression of that type can produce.



## Supported data types

Below is the list of data types supported.

| #    | Data type | Description                                                  |
| ---- | --------- | ------------------------------------------------------------ |
| 1    | bigint    | The int type.                                                |
| 2    | float     | The float type.                                              |
| 3    | string    | Text values, comprised of Unicode characters.                |
| 4    | datetime  | datatime type - *Currently it's NOT supported yet*.          |
| 5    | boolean   | The boolean type, the value could be ``true`` or ``false``.  |
| 6    | array     | The array type, can be any types from simple data or struct type (#1 - #5, and #7). |
| 7    | struct    | The complex type. Set of name/value pairs. Values must be of supported data type. |



## Type conversions

These are the rules governing *data type conversions*:

- ...
- 

