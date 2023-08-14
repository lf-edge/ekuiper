# 数组函数

数组函数用于操作数组或返回有关数组的信息。

## CARDINALITY

```text
cardinality(array)
```

返回数组中的元素数。

## ARRAY_POSITION

```text
array_position(array, value)
```

返回第二个参数在列表参数中的索引下标位置，索引下标从 0 开始，若该元素不存在，则返回 -1。

## ELEMENT_AT

```text
element_at(array, index)
```

返回列表参数中在给定索引下的元素，索引下标从 0 开始，若该索引小于 0，则该元素从列表末向列表头进行计数。

## ARRAY_CONTAINS

```text
array_contains(array, value)
```

返回给定元素是否存在列表参数中，存在则返回 true，否则返回 false。

## ARRAY_CREATE

```text
array_create(value1, ......)
```

将给定的元素参数们创建为一个列表元素。

## ARRAY_REMOVE

```text
array_remove(array, value)
```

返回删除了所有出现的给定元素的数组。

## ARRAY_LAST_POSITION

```text
array_last_position(array, val)
```

返回第二个参数在列表参数中最后一次出现的下标位置，索引下标从 0 开始，若该元素不存在，则返回 -1。

## ARRAY_CONTAINS_ANY

```text
array_contains_any(array1, array2)
```

返回第一个参数中是否存在第二个参数中的任意一个元素，存在则返回 true，否则返回 false。

## ARRAY_INTERSECT

```text
array_intersect(array1, array2)
```

返回两个数组的交集，且不包含重复元素。

## ARRAY_UNION

```text
array_union(array1, array2)
```

返回两个数组的并集，且不包含重复元素。

## ARRAY_MAX

```text
array_max(array)
```

返回数组中的最大值, 数组元素中的 null 值将被忽略。

## ARRAY_MIN

```text
array_min(array)
```

返回数组中的最小值, 数组元素中的 null 值将被忽略。

## ARRAY_EXCEPT

```text
array_except(array1, array2)
```

返回第一个数组中存在，但第二个数组中不存在的元素，且不包含重复元素。

## REPEAT

```text
repeat(value, count)
```

返回一个由给定元素参数重复出现指定次数的列表元素。

## SEQUENCE

```text
sequence(start, stop[, step])
```

返回一个从第一个开始参数到第二个结束参数的整数列表，每个元素按照给定的步长递增或递减。若未提供步长，则默认为
1（如果第一个开始参数小于第二个结束参数），或 -1（如果第一个开始参数大于第二个结束参数），且步长不允许为 0。

## ARRAY_CARDINALITY

```text
array_cardinality(array)
```

返回数组中的元素数。数组中的 null 值不计算在内。

## ARRAY_FLATTEN

```text
array_flatten(array)
```

返回一个扁平化的数组，即将数组中的数组元素展开。

例如，传入参数为 [[1, 4], [2, 3]]，则返回 [1, 4, 2, 3]。

## ARRAY_DISTINCT

```text
array_distinct(array)
```

返回一个去重的数组，即将数组中的重复元素去除。

## ARRAY_MAP

```text
array_map(function_name, array)
```

返回一个新的数组，其中包含对给定数组中的每个元素应用给定函数的结果。

## ARRAY_JOIN

```text
array_join(array, delimiter, null_replacement)
```

返回一个字符串，其中包含给定数组中的所有元素，元素之间用给定的分隔符分隔。如果数组中的元素为 null，则用给定的 null_replacement 替换。

例如，传入参数为 [1, 2, 3]，delimiter 设置为逗号，则返回 “1,2,3”。

## ARRAY_SHUFFLE

```text
array_shuffle(array)
```

返回一个随机排序的数组。

## ARRAY_CONCAT

```text
array_concat(array1, array2, ...)
```

用于合并两个或多个数组。此函数不会更改现有数组，而是返回一个新的数组。

## ARRAY_SORT

```text
array_sort(array)
```

返回输入数组的排序副本。

```sql
array_sort([3, 2, "b", "a"])
```

结果:

```sql
[2, 3, "a", "b"]
```
