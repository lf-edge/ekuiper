# Array Functions

Array functions manipulate arrays or return information about arrays.

## CARDINALITY

```text
cardinality(array)
```

The number of members in the array. The null value will return 0.

## ARRAY_POSITION

```text
array_position(array, value)
```

Return a 0-based index of the first occurrence of val if it is found within an array. If val does not exist within the
array, it returns -1.

## ELEMENT_AT

```text
element_at(array, index)
```

Returns element of the array at index val. If val < 0, this function accesses elements from the last to the first.

## ARRAY_CONTAINS

```text
array_contains(array, value)
```

Returns true if array contains the element.

## ARRAY_CREATE

```text
array_create(value1, ......)
```

Construct an array from literals.

## ARRAY_REMOVE

```text
array_remove(array, value)
```

Returns the array with all occurrences of value removed.

## ARRAY_LAST_POSITION

```text
array_last_position(array, val)
```

Return a 0-based index of the last occurrence of val if it is found within the array. If val does not exist within the
array, it returns -1.

## ARRAY_CONTAINS_ANY

```text
array_contains_any(array1, array2)
```

Returns true if array1 and array2 have any elements in common.

## ARRAY_INTERSECT

```text
array_intersect(array1, array2)
```

Returns an intersection of the two arrays, with all duplicates removed.

## ARRAY_UNION

```text
array_union(array1, array2)
```

Returns a union of the two arrays, with all duplicates removed.

## ARRAY_MAX

```text
array_max(array)
```

Returns an element which is greater than or equal to all other elements of the array. The null element will be ignored.

## ARRAY_MIN

```text
array_min(array)
```

Returns an element which is less than or equal to all other elements of the array. The null element will be ignored.

## ARRAY_EXCEPT

```text
array_except(array1, array2)
```

Returns an array of elements that are in array1 but not in array2, without duplicates.

## REPEAT

```text
repeat(string, count)
```

Constructs an array of val repeated count times.

## SEQUENCE

```text
sequence(start, stop, step)
```

Returns an array of integers from start to stop, incrementing by step.

## ARRAY_CARDINALITY

```text
array_cardinality(array)
```

Return the number of elements in the array. The null value will be ignored.

## ARRAY_FLATTEN

```text
array_flatten(array)
```

Return a flattened array, i.e., expand the array elements in the array.

For example, if the input is [[1, 4], [2, 3]], then the output is [1, 4, 2, 3].

## ARRAY_DISTINCT

```text
array_distinct(array)
```

Return a distinct array, i.e., remove the duplicate elements in the array.

## ARRAY_MAP

```text
array_map(function_name, array)
```

Return a new array by applying a function to each element of the array.

## ARRAY_JOIN

```text
array_join(array, delimiter, null_replacement)
```

Return a string that concatenates all elements of the array and uses the delimiter and an optional string to replace null values.

For example, if the input is [1, 2, 3], delimiter is set to comma, then the output is "1,2,3".

## ARRAY_SHUFFLE

```text
array_shuffle(array)
```

Return a shuffled array, i.e., randomly shuffle the elements in the array.

## ARRAY_CONCAT

```text
array_concat(array1, array2, ...)
```

Returns the concatenation of the input arrays, this function does not modify the existing arrays, but returns new one.

## ARRAY_SORT

```text
array_sort(array)
```

Returns a sorted copy of the input array.

```sql
array_sort([3, 2, "b", "a"])
```

Result:

```sql
[2, 3, "a", "b"]
```
