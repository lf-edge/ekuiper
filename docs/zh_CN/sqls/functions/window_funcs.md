# 窗口函数

窗口函数用于对数据进行聚合操作，并将结果添加到每一行数据中。目前，窗口函数目前只能被用在 select field 中。

## ROW_NUMBER

```text
row_number()
```

row_number() 将从 1 开始，为每一条记录返回一个数字。
