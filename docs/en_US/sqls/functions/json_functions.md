# JSON Functions

JSON functions manipulate JSON string or return information about JSON.
There are several json path manipulation functions. **Please refer
to [json path functions](../json_expr.md#json-path-functions) for how to compose a json path.**

## TO_JSON

```
to_json(col)
```

Converts a value to a string containing the JSON representation of the value. If the input is NULL, the result is also
NULL.

## PARSE_JSON

```
parse_json(col)
```

Converts a JSON string to a value. If the input is NULL, the result is also NULL.

## JSON_PATH_EXISTS

```
json_path_exists(col, json_path)
```

Check whether JSON path returns any item for the specified JSON value. Return bool value.

## JSON_PATH_QUERY

```
json_path_query(col, json_path)
```

Get all items returned by JSON path for the specified JSON value.

## JSON_PATH_QUERY_FIRST

```
json_path_query_first(col, json_path)
```

Get the first item returned by JSON path for the specified JSON value.