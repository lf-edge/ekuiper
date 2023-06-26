# JSON 函数

JSON 函数操作 JSON 字符串或返回有关 JSON 的信息。
这里面包含了几个 JSON PATH 操作函数。**请参阅 [json 路径函数](../json_expr.md#Json-路径函数) 了解如何编写json路径。**

## TO_JSON

```text
to_json(col)
```

将输入值转换为包含该值 JSON 表示的字符串。如果输入为 NULL，则结果也为 NULL。

## PARSE_JSON

```text
parse_json(col)
```

将输入的 JSON 字符串转换为值。如果输入为 NULL，则结果也为 NULL。

## JSON_PATH_EXISTS

```text
json_path_exists(col, json_path)
```

检查 JSON 路径是否返回指定 JSON 值的任何项目。 返回布尔值。

## JSON_PATH_QUERY

```text
json_path_query(col, json_path)
```

获取 JSON 路径返回的指定 JSON 值的所有项目。

## JSON_PATH_QUERY_FIRST

```text
json_path_query_first(col, json_path)
```

获取 JSON 路径返回的指定 JSON 值的第一个项目。
