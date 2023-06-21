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

## ARRAY_CONTAIN_ANY

```text
array_contain_any(array1, array2)
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

Returns an element which is greater than or equal to all other elements of the array. If an element of the array is
null, it returns null.

## ARRAY_MIN

```text
array_min(array)
```

Returns an element which is less than or equal to all other elements of the array. If an element of the array is null,
it returns null.

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
