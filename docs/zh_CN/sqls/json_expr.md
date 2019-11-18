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

源引用运算符可用于通过引用源流或表来指定列。 ``->``引用选择嵌套JSON对象中的键。

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

索引表达式使您可以选择列表中的特定元素。 它看起来应该类似于普通编程语言中的数组访问。 索引从0开始。

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

### 切片

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



### *映射* -不支持

#### 列表和切片映射

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

#### 对象映射

```
SELECT ops->*->numArgs FROM demo

{ "numArgs" : [2, 3] }
```

