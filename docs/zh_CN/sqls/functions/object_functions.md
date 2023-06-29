# 对象函数

对象函数用于操作对象/映射。

## KEYS

```text
keys(map<string, any>)
```

返回的给定的 map 参数中的所有 key 值。

## VALUES

```text
values(map<string, any>)
```

返回给定的 map 参数中的所有 value 值。

## OBJECT

```text
object(arr1, arr2)
```

接受两个 list 参数来构造 map 对象，第一个 list 作为 map 对象的 key，第二个 list 作为 map 对象的 value。两个 list 参数长度必须相等。

## ZIP

```text
zip([arr1, arr2], ......)
```

接受一组 list 对象来构造 map 对象，每个 list 元素的长度必须为 2，每个 list 元素内的第一个元素将作为 key，第二个元素将作为
value。

## ITEMS

```text
items(map<string, any>)
```

根据给定的 map 参数构造一个 list 对象，每个元素都为一个长度为 2 的 list 对象，其中第一个元素为 key，第二个元素为 value。

## OBJECT_CONSTRUCT

```text
object_construct(key1, col, ...)
```

返回由参数构建的 object/map 。参数为一系列的键值对，因此必须为偶数个。键必须为 string 类型，值可以为任意类型。如果值为空，则该键值对不会出现在最终的对象中。
