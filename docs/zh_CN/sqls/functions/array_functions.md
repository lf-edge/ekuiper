# 数组函数

数组函数用于操作数组或返回有关数组的信息。

## CARDINALITY

```
cardinality(array)
```

返回数组中的元素数。

## ARRAY_POSITION

```
array_position(array, value)
```

返回第二个参数在列表参数中的索引下标位置，索引下标从 0 开始，若该元素不存在，则返回 -1。

## ELEMENT_AT

```
element_at(array, index)
```

返回列表参数中在给定索引下的元素，索引下标从 0 开始，若该索引小于 0，则该元素从列表末向列表头进行计数。

## ARRAY_CONTAINS

```
array_contains(array, value)
```

返回给定元素是否存在列表参数中，存在则返回 true，否则返回 false。

## ARRAY_CREATE

```
array_create(value1, ......)
```

将给定的元素参数们创建为一个列表元素。

## ARRAY_REMOVE

```
array_remove(array, value)
```

返回删除了所有出现的给定元素的数组。

## ARRAY_LAST_POSITION

```
array_last_position(array, val)
```

返回第二个参数在列表参数中最后一次出现的下标位置，索引下标从 0 开始，若该元素不存在，则返回 -1。

## ARRAY_CONTAIN_ANY

```
array_contain_any(array1, array2)
```

返回第一个参数中是否存在第二个参数中的任意一个元素，存在则返回 true，否则返回 false。

## ARRAY_INTERSECT

```
array_intersect(array1, array2)
```

返回两个数组的交集，且不包含重复元素。

## ARRAY_UNION

```
array_union(array1, array2)
```

返回两个数组的并集，且不包含重复元素。

## ARRAY_MAX

```
array_max(array)
```

返回数组中的最大值, 若数组元素中存在 null，则返回 null。

## ARRAY_MIN

```
array_min(array)
```

返回数组中的最小值, 若数组元素中存在 null，则返回 null。

## ARRAY_EXCEPT

```
array_except(array1, array2)
```

返回第一个数组中存在，但第二个数组中不存在的元素，且不包含重复元素。

## REPEAT

```
repeat(value, count)
```

返回一个由给定元素参数重复出现指定次数的列表元素。

## SEQUENCE

```
sequence(start, stop[, step])
```

返回一个从第一个开始参数到第二个结束参数的整数列表，每个元素按照给定的步长递增或递减。若未提供步长，则默认为
1（如果第一个开始参数小于第二个结束参数），或 -1（如果第一个开始参数大于第二个结束参数），且步长不允许为 0。