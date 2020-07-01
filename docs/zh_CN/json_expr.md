# 样本数据

```json
{
  "name": {"first": "Tom", "last": "Anderson"},
  "age":37,
  "children": ["Sara","Alex","Jack"],
  "fav.movie": "Deer Hunter",
  "friends": [
    {"first": "Dale", "last": "Murphy", "age": 44},
    {"first": "Roger", "last": "Craig", "age": 68},
    {"first": "Jane", "last": "Murphy", "age": 47}
  ],
    "followers": {
        "Group1": [
		    {"first": "John", "last": "Shavor", "age": 22},
		    {"first": "Ken", "last": "Miller", "age": 33}
        ],
        "Group2": [
            {"first": "Alice", "last": "Murphy", "age": 33},
		    {"first": "Brian", "last": "Craig", "age": 44}
        ]
    }
   "ops": {
   	"functionA": {"numArgs": 2},
    "functionB": {"numArgs": 3},
    "functionC": {"variadic": true}
  }
}
```

# 基本表达式

## 识别码

源引用（`.`）源解引用运算符可用于通过引用源流或表来指定列。'->引用选择嵌套JSON对象中的键。

```
SELECT demo.age FROM demo
{"age" : 37}
```



```
SELECT demo.name->first FROM demo
{"first" : "Tom"}
```



```
SELECT name->first AS fname FROM demo
{"fname": "Tom"}
```

## 索引表达式

索引表达式使您可以选择列表中的特定元素。 它看起来应该类似于普通编程语言中的数组访问。 索引基于0。

```
SELECT children FROM demo
{
    "children": ["Sara","Alex","Jack"]
}
```



```
SELECT children[0] FROM demo
{
    "children": "Sara"
}

SELECT d.friends[0]->last FROM demo AS d
{
    "last" : "Murphy"
}
```

# 切片

切片允许您选择数组的连续子集。

``field[from:to]`` 如果未指定from，则表示从数组的第一个元素开始; 如果未指定to，则表示以数组的最后一个元素结尾。

```
SELECT children[0:1] FROM demo
{
    "children": ["Sara","Alex"]
}
```



```
SELECT children[:] FROM demo == SELECT children FROM demo
{
    "children": ["Sara","Alex","Jack"]
}
```



```
SELECT children[:1] FROM demo
{
    "children": ["Sara","Alex"]
}
```



```
SELECT followers->Group1[:1]->first FROM demo

{
    "first": ["John","Alice"]
}
```

# Json Path functions

Kuiper provides a list of functions to allow to execute json path over struct or array columns or values. The functions are:

```tsql
json_path_exists(col, jsonpath)
json_path_query(col, jsonpath)
json_path_query_first(col, jsonpath)
```

Please refer to [json functions](sqls/built-in_functions.md#json-functions) for detail.

All these functions share the same parameter signatures, among which, the second parameter is a jsonpath string. The jsonpath grammer used by Kuiper is based on [JsonPath](https://goessner.net/articles/JsonPath/).  

The basic grammar of those expressions is to use the keys part of the JSON objects combined with some elements:

- Dots `.` to move into a tree
- Brackets `[]` for access to a given array member coupled with a position. It can also access to a map field.
- Variables, with `$` representing a JSON text and `@` for result path evaluations.

So for example, when applied to the previous JSON data sample we can reach the following parts of the tree with these expressions:

- `$.age` refers to 37.
- `$.friends.first` refers to “dale”.
- `$.friends` refers to the full array of friends.
- `$.friends[0]` refers to the first friend listed in the previous array (contrary to arrays members are zero-based).
- `$.friends[0][lastname]` refers to the lastname of the first friend listed. Use bracket if [there are reserved words](sqls/lexical_elements.md) or special characters (such as space ' ', '.' and Chinese etc) in the field key.
- `$.friends[? @.age>60].first` or `$.friends[? (@.age>60)].first` refers to the first name of the friends whose age is bigger than 60. Notice that the space between ? and the condition is required even the condition is with braces.

Developers can use the json functions in the SQL statement. Here are some examples.

- Select the lastname of group1 followers
```tsql
SELECT json_path_query(followers, "$.Group1[*].last") FROM demo

["Shavor","Miller"]
```

- Select the lastname if any of the group1 followers is older than 60
```tsql
SELECT name->last FROM demo where json_path_exists(followers, "$.Group1[? @.age>30]")

"Anderson"
```

- Select the follower's lastname from group1 whose age is bigger than 30
```tsql
SELECT json_path_exists(followers, "$.Group1[? @.age>30].last") FROM demo

["Miller"]
```

- Assume there is a field in follows with reserved words or chars like dot `my.follower`, use bracket to access it.
```tsql
SELECT json_path_exists(followers, "$[\"my.follower\"]") FROM demo

["Miller"]
```



# *映射*

<!--Do we need to support this?-->

## 列表和切片映射

通配符表达式创建列表映射，它是JSON数组上的映射。

```
SELECT demo.friends[*]->first FROM demo
{
    "first": ["Dale", "Roger", "Jane"]
}
```



```
SELECT friends[:1]->first FROM demo
{
    "first": ["Dale", "Roger"]
}
```

## 对象映射



```
SELECT ops->*->numArgs FROM demo

{ "numArgs" : [2, 3] }
```

