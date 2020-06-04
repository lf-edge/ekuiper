# Sample data

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

# Basic expressions

## Identifier 

Source Dereference (`.`) The source dereference operator can be used to specify columns by dereferencing the source stream or table. The ``->`` dereference selects a key in a nested JSON object.

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

## Index expression

Index Expressions allow you to select a specific element in a list. It should look similar to array access in common programming languages. Indexing is 0 based.

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

# Slicing

Slices allow you to select a contiguous subset of an array. 

``field[from:to]`` If from is not specified, then it means start from the 1st element of array; If to is not specified, then it means end with the last element of array.

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
- Brackets `[]` for access to a given array member coupled with a position.
- Variables, with `$` representing a JSON text and `@` for result path evaluations.

So for example, when applied to the previous JSON data sample we can reach the following parts of the tree with these expressions:

- `$.age` refers to 37.
- `$.friends.first` refers to “dale”.
- `$.friends` refers to the full array of friends.
- `$.friends[0]` refers to the first friend listed in the previous array (contrary to arrays members are zero-based).
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

# *Projections*

<!--Do we need to support this?-->

## List & Slice projections

A wildcard expression creates a list projection, which is a projection over a JSON array. 

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

## Object projections



```
SELECT ops->*->numArgs FROM demo

{ "numArgs" : [2, 3] }
```

