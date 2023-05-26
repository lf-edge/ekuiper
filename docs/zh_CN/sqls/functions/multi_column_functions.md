# 多列函数

多列函数执行运算之后会返回多个列。相对来说，普通的标量函数只返回单列。

多列函数仅可在 `SELECT` 子句中使用。

## CHANGED_COLS

```
changed_cols(prefix, ignoreNull, colA, colB)
```

返回值有变化的列，列名添加指定前缀。请看 [changed_cols](./analytic_functions.md#changedcols-函数) 了解更多用法。