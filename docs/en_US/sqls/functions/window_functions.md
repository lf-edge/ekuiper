# Window Functions

A window function performs a calculation across a set of table rows that are somehow related to the current row. This is comparable to the type of calculation that can be done with an aggregate function. For now, window functions can only be used in select fields.

## ROW_NUMBER

```text
row_number()
```

ROW_NUMBER numbers all rows sequentially (for example 1, 2, 3, 4, 5).
