# Multiple Row Functions

A multiple row function is a function that returns multiple rows.

Multiple row function can only be used in the `SELECT` clause of a query and only allowed 1 multiple rows function in
the clause for now.

## UNNEST

```text
unnest(array)
```

The `unnest` function is used to expand an array into multiple rows.
The argument column must be an array. This function will expand the array into multiple rows as a returned result. If
the item in the array is map[string]interface object, then it will be built as columns in the result rows.

### Examples

Create a stream demo and have below inputs

```json lines
{
  "a": [
    1,
    2
  ],
  "b": 3
}
```

Rule to get the unnest values:

```text
SQL: SELECT unnest(a) FROM demo
___________________________________________________
{"unnest":1}
{"unnest":2}
```

Rule to get the unnest values with other columns:

```text
SQL: SELECT unnest(a), b FROM demo
___________________________________________________
{"unnest":1, "b":3}
{"unnest":2, "b":3}
```

Create a stream demo and have below inputs

```json lines
{
  "x": [
    {
      "a": 1,
      "b": 2
    },
    {
      "a": 3,
      "b": 4
    }
  ],
  "c": 5
}
```

Rule to get the unnest values with other columns:

```text
SQL: SELECT unnest(x), c FROM demo
___________________________________________________
{"a":1, "b":2, "c": 5}
{"a":3, "b":4, "c": 5}
```
