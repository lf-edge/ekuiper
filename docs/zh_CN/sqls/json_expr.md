# JSON 表达式

**采样数据**

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

## 基本表达式

### 标识符

源引用（`.`）

源引用运算符可用于通过引用源流或表来指定列。 `->` 引用选择嵌套 JSON 对象中的键。

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

### 索引表达式

索引表达式使您可以选择列表中的特定元素。 它看起来应该类似于普通编程语言中的数组访问。 索引值以0为开始值，-1 为从末尾的开始位置，以此类推。

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

SELECT children[1] FROM demo

{
    "children": "Alex"
}

SELECT children[-1] FROM demo

{
    "children": "Jack"
}

SELECT children[-2] FROM demo

{
    "children": "Alex"
}

SELECT d.friends[0]->last FROM demo AS d

{
    "last" : "Murphy"
}
```

### 切片

切片允许您选择数组的连续子集。

`field[from:to)` 为前闭后开区间，不包含to。如果未指定 from，则表示从数组的第一个元素开始; 如果未指定 to，则表示以数组的最后一个元素结尾。

```
SELECT children[0:1] FROM demo

{
    "children": ["Sara"]
}

SELECT children[1:-1] FROM demo

{
    "children": ["Alex"]
}

SELECT children[0:-1] FROM demo

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
SELECT children[:2] FROM demo

{
    "children": ["Sara","Alex"]
}

SELECT children[:-1] FROM demo

{
    "children": ["Sara","Alex"]
}
```



```
SELECT followers->Group1[:1]->first FROM demo

{
    "first": ["John"]
}
```

## Json 路径函数

eKuiper 提供了一系列函数，以允许通过结构或数组列或值进行 json 路径操作。 这些函数是：

```sql
json_path_exists(col, jsonpath)
json_path_query(col, jsonpath)
json_path_query_first(col, jsonpath)
```

请参考 [json 函数](./built-in_functions.md#JSON-函数) 获得详细信息.

所有这些函数共享相同的参数签名，其中，第二个参数是 jsonpath 字符串。 eKuiper 使用的 jsonpath 语法基于[JsonPath](https://goessner.net/articles/JsonPath/)

这些表达式的基本语法是将 JSON 对象的字段部分与一些元素结合使用：

- 点 `.` 用于移动到树中
- 括号 `[]` 用于访问给定位置的数组成员。 它还可以访问地图字段。
- 变量，`$` 表示 JSON 文本，`@` 表示结果路径计算。

例如，当应用于上述的 JSON 数据示例时，我们可以使用这些表达式访问树的以下部分：

- `$.age` 指的是37。
- `$.friends.first` 指的是 "dale"。
- `$.friends` 指的是完整的朋友数组。
- `$.friends[0]` 指的是上一个数组中列出的第一个朋友（与数组成员相反，它们从零开始）。
- `$.friends[0][lastname]` 是指列出的第一个朋友的姓氏.。如果 fields key 中有 [保留字](./lexical_elements.md)或特殊字符（例如空格 ' '、 '.'和中文等），请使用括号。
- `$.friends[? @.age>60].first` 或者 `$.friends[? (@.age>60)].first` 是指年龄大于60岁的朋友的名字。请注意，？之间有空格， 并且条件是必需的，即使条件带有括号。

开发人员可以在 SQL 语句中使用 json 函数。 这里有些例子。

- 查询第1组跟随者的姓氏
```sql
SELECT json_path_query(followers, "$.Group1[*].last") FROM demo

["Shavor","Miller"]
```

- 查询第1组年龄大于60岁的跟随者的姓氏
```sql
SELECT name->last FROM demo where json_path_exists(followers, "$.Group1[? @.age>30]")

"Anderson"
```

- 查询第1组年龄大于30岁的跟随者的姓氏
```sql
SELECT json_path_exists(followers, "$.Group1[? @.age>30].last") FROM demo

["Miller"]
```

- 假设跟随者有一个字段有保留字或点之类的字符， 比如 `my.follower`,使用括号访问它。
```sql
SELECT json_path_exists(followers, "$[\"my.follower\"]") FROM demo

["Miller"]
```

### *映射* -不支持

#### 列表和切片映射

通配符表达式创建列表映射，它是 JSON 数组上的映射。

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

#### 对象映射

```
SELECT ops->*->numArgs FROM demo

{ "numArgs" : [2, 3] }
```

