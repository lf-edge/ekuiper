# JSON Expressions

**Sample data**

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

## Basic expressions

### Identifier 

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

### Index expression

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

### Slicing

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



### *Projections* - *NOT SUPPORT YET*

#### List & Slice projections

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

#### Object projections

```
SELECT ops->*->numArgs FROM demo

{ "numArgs" : [2, 3] }
```

