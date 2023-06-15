# 多行函数

多列函数执行运算之后会返回多个行。

多行函数仅可在 `SELECT` 子句中使用, 并且在 `SELECT` 子句中只允许使用一个多行函数。

## UNNEST

```text
unnest(array)
```

参数必须是一个 array 对象。该函数将参数 array 展开成多行作为结果返回。如果 array 对象中每一个子项为 map[string]interface{}
对象，则该子项会作为列在返回的行中。

### 例子

创建流 demo，并给与如下输入。

```json lines
{
  "a": [
    1,
    2
  ],
  "b": 3
}
```

获取 unnest 结果的规则:

```text
SQL: SELECT unnest(a) FROM demo
___________________________________________________
{"unnest":1}
{"unnest":2}
```

获取 unnest 结果与其他列的规则:

```text
SQL: SELECT unnest(a), b FROM demo
___________________________________________________
{"unnest":1, "b":3}
{"unnest":2, "b":3}
```

创建流 demo，并给与如下输入。

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

获取 unnest 结果与其他列的规则:

```text
SQL: SELECT unnest(x), c FROM demo
___________________________________________________
{"a":1, "b":2, "c": 5}
{"a":3, "b":4, "c": 5}
```
