# Multiple Column Functions

A multiple column function is a function that returns multiple columns. Contrast to normal scalar function, which
returns a single column of a single row.

Multiple column function can only be used in the `SELECT` clause of a query.

## CHANGED_COLS

```
changed_cols(prefix, ignoreNull, colA, colB)
```

Return the changed columns whose name is prefixed. Check [changed_cols](./analytic_functions.md#changedcols-function)
for detail.
