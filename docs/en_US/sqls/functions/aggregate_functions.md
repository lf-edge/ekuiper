# Aggregate Functions

Aggregate functions perform a calculation on a set of values and return a single value. Aggregate functions can be used
as expressions only in the following:

* The select list of a SELECT statement (either a sub-query or an outer query).
* A HAVING clause.

## AVG

```text
avg(col)
```

The average of the values in a group. The null values will be ignored.

## COUNT

```text
count(*)
count(col)
```

The number of items in a group. The null values will be ignored.

## MAX

```text
max(col)
```

The maximum value in a group. The null values will be ignored.

## MIN

```text
min(col)
```

The minimum value in a group. The null values will be ignored.

## SUM

```text
sum(col)
```

The sum of all the values in a group. The null values will be ignored.

## COLLECT

```text
collect(*)
collect(col)
```

Returns an array with all columns or the whole record (when the parameter is *) values from the group.

Examples:

* Get an array of column `a` of the current window. Assume the column `a` is of an int type, the result will be
  like: `[{"r1":[32, 45]}]`

    ```sql
    SELECT collect(a) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

* Get the whole array of the current window. The result will be
  like: `[{"r1":[{"a":32, "b":"hello"}, {"a":45, "b":"world"}]}]`

    ```sql
    SELECT collect(*) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

* Get the second element's column 'a' value within the current window. The result will be like: `[{"r1":32}]`

    ```sql
    SELECT collect(*)[1]->a as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

## MERGE_AGG

```text
merge_agg(*)
merge_agg(col)
```

Concatenate values from the group into a single value.
It concatenates multiple objects by generating an object containing the union of their keys,
taking the second object's value when there are duplicate keys.
It does not operate recursively; only the top-level object structure is merged.

If the parameter is a column,
the result will be an object containing the union of the keys of all the objects in the column.
If the column contains only non-object values, the result will be an empty object.

## Examples

Given the following values in the group:

```json lines
{
  "a": {
    "a": 2
  },
  "b": 2,
  "c": 3
}
{
  "a": {
    "b": 2
  },
  "b": 5,
  "d": 6
}
{
  "a": {
    "a": 3
  },
  "b": 8
}
```

* Concat wildcard, the result will be: `{"a": {"a": 3}, "b": 8, "c": 3, "d": 6}`

    ```sql
    SELECT merge_agg(*) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

* Concat a specified object column, the result will be: `{"a": 3, "b": 2}`

    ```sql
    SELECT merge_agg(a) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

* Concat a specified non-object column, the result will be: `{}`

    ```sql
    SELECT merge_agg(b) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

## DEDUPLICATE

```text
deduplicate(col, false)
```

Returns the deduplicate results in the group, usually a window. The first argument is the column as the key to
deduplicate; the second argument is whether to return all items or just the latest item which is not duplicate. If the
latest item is a duplicate, the sink will receive an empty map. Set the sink
property [omitIfEmpty](../../guide/sinks/overview.md#common-properties) to the sink to not triggering the action.

Examples:

* Get the whole array of the current window which is deduplicated by column `a`. The result will be
  like: `[{"r1":{"a":32, "b":"hello"}, {"a":45, "b":"world"}}]`

    ```sql
    SELECT deduplicate(a, true) as r1 FROM test GROUP BY TumblingWindow(ss, 10)
    ```

* Get the column `a` value which is not duplicate during the last hour. The result will be
  like: `[{"r1":32}]`, `[{"r1":45}]` and `[{}]` if a duplicate value arrives. Use the omitIfEmpty sink property to
  filter out those empty results.

     ```sql
     SELECT deduplicate(a, false)->a as r1 FROM demo GROUP BY SlidingWindow(hh, 1)
     ```

## STDDEV

```text
stddev(col)
```

Returns the population standard deviation of expression in the group, usually a window. The argument is the column as
the key to stddev.

## STDDEVS

```text
stddevs(col)
```

Returns the sample standard deviation of expression in the group, usually a window. The argument is the column as the
key to stddevs.

## VAR

```text
var(col)
```

Returns the population variance (square of the population standard deviation) of expression in the group, usually a
window. The argument is the column as the key to var.

## VARS

```text
vars(col)
```

Returns the sample variance (square of the sample standard deviation) of expression in the group, usually a window. The
argument is the column as the key to vars.

## PERCENTILE

```text
percentile(col, percentile)
```

Returns the percentile value based on a continuous distribution of expression in the group, usually a window. The first
argument is the column as the key to percentile. The second argument is the percentile of the value that you want to
find. The percentile must be a constant between 0.0 and 1.0.

## PERCENTILE_DISC

```text
percentile_disc(col, percentile)
```

Returns the percentile value based on a discrete distribution of expression in the group, usually a window. The first
argument is the column as the key to percentile_disc. The second argument is the percentile of the value that you want
to find. The percentile must be a constant between 0.0 and 1.0.
