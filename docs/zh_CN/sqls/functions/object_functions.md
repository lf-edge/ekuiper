# 对象函数

对象函数用于操作对象/映射。

## KEYS

```text
keys(obj)
```

返回的给定的 map 参数中的所有 key 值, 举例如下:

```sql
keys({"a":1, "b":2})
```

得到如下结果:

```sql
["a","b"]
```

## VALUES

```text
values(obj)
```

返回给定的 map 参数中的所有 value 值,举例如下:

```sql
values({"a":1, "b":2})
```

得到如下结果:

```sql
[1,2]
```

## OBJECT

```text
object(keys, values)
```

接受两个 list 参数来构造 map 对象，第一个 list 作为 map 对象的 key，第二个 list 作为 map 对象的 value。两个 list 参数长度必须相等, 举例如下:

```sql
object(["a","b"],[1,2])
```

得到如下结果:

```sql
{"a":1, "b":2}
```

## ZIP

```text
zip(entries)
```

接受一组 list 对象来构造 map 对象，每个 list 元素的长度必须为 2，每个 list 元素内的第一个元素将作为 key，第二个元素将作为
value, 举例如下:

```sql
zip([["a",1],["b":2]])
```

得到如下结果:

```sql
{"a":1, "b":2}
```

## ITEMS

```text
items(obj)
```

根据给定的 map 参数构造一个 list 对象，每个元素都为一个长度为 2 的 list 对象，其中第一个元素为 key，第二个元素为 value，举例如下:

```sql
items({"a":1, "b":2})
```

得到如下结果:

```sql
[["a",1],["b":2]]
```

## OBJECT_CONSTRUCT

```text
object_construct(key1, col, ...)
```

返回由参数构建的 object/map 。参数为一系列的键值对，因此必须为偶数个。键必须为 string 类型，值可以为任意类型。如果值为空，则该键值对不会出现在最终的对象中，举例如下:

```sql
object_construct("a", 1, "b", 2)
```

得到如下结果:

```sql
{"a":1, "b":2}
```

## OBJECT_CONCAT

```text
object_concat(obj1, obj2, ...)
```

该函数是一个连接输入对象并返回新对象的函数。该函数至少需要两个输入对象作为参数。当输入对象中存在相同属性名称时，函数将选择输入列表中最后一个相关对象的属性，并将其复制到输出对象中。以下是一个示例：

```sql
object_concat({"a": 1}, {"b": 2}, {"b": 3})
```

得到如下结果:

```sql
{"a":1, "b":3}
```

## ERASE

```text
erase(obj, k)
```

如果 k 是一个字符串，则返回一个新对象，其中键 k 被删除。如果 k 是一个字符串数组，则返回一个新对象，其中包含 k 中的键被删除。

```sql
erase({"baz": [1, 2, 3], "bar": 'hello world',"foo":'emq'}, 'foo')
```

得到如下结果:

```sql
{"baz": [1, 2, 3], "bar": 'hello world'}
```
