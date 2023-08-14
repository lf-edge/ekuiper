# Object Functions

Object functions are used to manipulate objects/maps.

## KEYS

```text
keys(obj)
```

Return an array containing the keys of the map.

example:

```sql
keys({"a":1, "b":2})
```

result:

```sql
["a","b"]
```

## VALUES

```text
values(obj)
```

example:

```sql
values({"a":1, "b":2})
```

result:

```sql
[1,2]
```

Return an array containing the values of the map.

## OBJECT

```text
object(keys, values)
```

Construct an object from an array of keys and an array of values. Keys must be an array of strings. Values must be an
arbitrary array of the same length as keys.

example:

```sql
object(["a","b"],[1,2])
```

result:

```sql
{"a":1, "b":2}
```

## ZIP

```text
zip(entries)
```

Construct an object from an array of entries. Each entry must itself be an array of size 2: the first element is the
key (and must be a string), and the second element is the value.

example:

```sql
zip([["a",1],["b":2]])
```

result:

```sql
{"a":1, "b":2}
```

## ITEMS

```text
items(obj)
```

Return an array containing the entries of object. Each entry is a 2-element array; the first is the key, the second is
the value.

example:

```sql
items({"a":1, "b":2})
```

result:

```sql
[["a",1],["b":2]]
```

## OBJECT_CONSTRUCT

```text
object_construct(key1, col1, key2, col2, ...)
```

Return a struct type object/map constructed by the arguments. The arguments are a series of key value pairs, thus the
arguments count must be an odd number. The key must be a string, and the value can be of any type. If the value is null,
the key/value pair will not present in the final object.

example:

```sql
object_construct("a", 1, "b", 2)
```

result:

```sql
{"a":1, "b":2}
```

## OBJECT_CONCAT

```text
object_concat(obj1, obj2, ...)
```

This function concatenates the input objects and returns a new object. It requires a minimum of two input objects. In cases where there are duplicate attribute names among the input objects, the attribute from the last relevant object in the input list is selected and copied to the output object. To illustrate, here's an example:

```sql
object_concat({"a": 1}, {"b": 2}, {"b": 3})
```

result:

```sql
{"a":1, "b":3}
```

## ERASE

```text
erase(obj, k)
```

If k is a string, return a new object where the key k is erased. If k is an array of strings, return a new object where the keys in k are erased.

```sql
erase({"baz": [1, 2, 3], "bar": 'hello world',"foo":'emq'}, 'foo')
```

result:

```sql
{"baz": [1, 2, 3], "bar": 'hello world'}
```
