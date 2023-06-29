# Object Functions

Object functions are used to manipulate objects/maps.

## KEYS

```text
keys(map<string, interface{}>) -> array<string>
```

Return an array containing the keys of the map.

## VALUES

```text
values(map<string, interface{}>) -> array<interface{}>
```

Return an array containing the values of the map.

## OBJECT

```text
object(array<string>, array<interface{}>) -> map<string, interface{}>
```

Construct an object from an array of keys and an array of values. Keys must be an array of strings. Values must be an
arbitrary array of the same length as keys.

## ZIP

```text
zip(array<array<string, interface{}>>) -> map<string, interface{}>
```

Construct an object from an array of entries. Each entry must itself be an array of size 2: the first element is the
key (and must be a string), and the second element is the value.

## ITEMS

```text
items(map<string, interface{}>) -> array<array<string, interface{}>>
```

Return an array containing the entries of object. Each entry is a 2-element array; the first is the key, the second is
the value.

## OBJECT_CONSTRUCT

```text
object_construct(key1, col1, key2, col2, ...) -> map<string, interface{}>
```

Return a struct type object/map constructed by the arguments. The arguments are a series of key value pairs, thus the
arguments count must be an odd number. The key must be a string, and the value can be of any type. If the value is null,
the key/value pair will not present in the final object.
