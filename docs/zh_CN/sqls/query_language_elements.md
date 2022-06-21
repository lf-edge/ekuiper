
# 查询语言元素

eKuiper 提供了用于构建查询的各种元素。 总结如下。

| 元素                    | 总结                                                                                                                             |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------|
| [SELECT](#select)     | SELECT 用于从输入流中检索行，并允许从 eKuiper 中的一个或多个输入流中选择一个或多个列。                                                                            |
| [FROM](#from)         | FROM 指定输入流。 任何 SELECT 语句始终需要 FROM 子句。                                                                                          |
| [JOIN](#join)         | JOIN 用于合并来自两个或更多输入流的记录。 JOIN 包括 LEFT，RIGHT，FULL 和 CROSS。JOIN 可用于多个流或者流和表格。当用于多个流时，必须运行在[窗口](./windows.md)中，否则每次单条数据，JOIN 没有意义。 |
| [WHERE](#where)       | WHERE 指定查询返回的行的搜索条件。                                                                                                           |
| [GROUP BY](#group-by) | GROUP BY 将一组选定的行分组为一组汇总行，这些汇总行按一个或多个列或表达式的值分组。该语句必须运行在[窗口](./windows.md)中。                                                     |
| [ORDER BY](#order-by) | 按一列或多列的值对行进行排序。                                                                                                                |
| [HAVING](#having)     | HAVING 为组或集合指定搜索条件。 HAVING 只能与 SELECT 表达式一起使用。                                                                                 |
|                       |                                                                                                                                |

## SELECT

从输入流中检索行，并允许从 eKuiper 中的一个或多个输入流中选择一个或多个列。

### 句法

```sql
SELECT 
	*
	| [source_stream.]column_name [AS column_alias]
	| expression
  
```

### 参数

指定应返回 FROM 子句中所有输入流的所有列。 列由输入源返回，如 FROM 子句中指定的那样，并按它们在传入流中存在的顺序或由 ORDER BY 子句指定的顺序返回。

**\***

从源流中选择所有字段。

**source_stream**

源流名称或别名。

**column_name**

要返回的列的名称。 如果要指定的列是嵌入式嵌套记录类型，则使用[JSON 表达式](json_expr.md)引用嵌入式列。

**column_alias**

用替代名称替换查询结果集中的列名称。 别名还用于指定表达式结果的名称。column_alias 不能在 WHERE、GROUP BY 或 HAVING 子句中使用。

**表达式**

表达式是一个常量、函数、或者由一个或多个运算符连接的列名、常量和函数的任意组合。

## FROM

指定输入流。 任何 SELECT 语句始终需要 FROM 子句。

### 句法

```sql
FROM source_stream | source_stream AS source_stream_alias 
```

### 参数

**source_stream | source_stream_alias**

输入流名称或别名。

## JOIN

JOIN 用于合并来自两个或更多输入流的记录。 JOIN 包括 LEFT，RIGHT，FULL 和CROSS。

### 句法

```sql
LEFT | RIGHT | FULL | CROSS 
JOIN 
source_stream | source_stream AS source_stream_alias
ON <source_stream|source_stream_alias>.column_name =<source_stream|source_stream_alias>.column_name
```

### 参数

**LEFT**

LEFT JOIN 关键字从左流（stream1）返回所有记录，并从右流（stream2）返回匹配的记录。 如果不匹配，则结果从右侧为 NULL。

```sql
SELECT column_name(s)
FROM stream1
LEFT JOIN stream2
ON stream1.column_name = stream2.column_name;
```

**RIGHT**

JOIN 关键字从右侧流（stream2）返回所有记录，并从左侧流（stream1）返回匹配的记录。 如果没有匹配项，则结果从左侧为 NULL。

```sql
SELECT column_name(s)
FROM stream1
RIGHT JOIN stream2
ON stream1.column_name = stream2.column_name;
```

**FULL**

当左（stream1）或右（stream2）表记录匹配时，FULL JOIN 关键字返回所有记录。

**注意：** FULL JOIN 可能返回较大的结果集！

```sql
SELECT column_name(s)
FROM stream1
FULL JOIN stream2
ON stream1.column_name = stream2.column_name
WHERE condition;
```

**CROSS**

CROSS JOIN 用于将第一个流（stream1）的每一行与第二个流（stream2）的每一行组合。 这也称为笛卡尔联接，因为它从联接表返回行集的笛卡尔乘积。 假设在 stream1中有 m 行，在stream2 中有 n 行，那么 CROSS JOIN 的结果将返回 m * n 行。

**注意：** CROSS JOIN 可能返回非常大的结果集！

```sql
SELECT column_name(s)
FROM stream1
CROSS OUTER JOIN stream2
ON stream1.column_name = stream2.column_name
WHERE condition;
```

**source_stream | source_stream_alias**

要连接的输入流名称或别名。

**column_name**

要返回的列的名称。 如果要指定的列是嵌入式嵌套记录类型，则使用[JSON 表达式](json_expr.md)引用嵌入式列。

## WHERE

WHERE 指定查询返回的行的搜索条件。 WHERE 子句仅用于提取满足指定条件的那些记录。

### 句法

```
WHERE <search_condition>
<search_condition> ::=   
    { <predicate> | ( <search_condition> ) }   
    [ { AND | OR } { <predicate> | ( <search_condition> ) } ]   
[ ,...n ]   
<predicate> ::=   
    { expression { = | < > | ! = | > | > = | < | < = | NOT IN} expression   
```

### 参数

表达式是一个常量、函数、及由一个或多个运算符连接的列名、常量和函数的任意组合。

**< search_condition >**

为SELECT语句或查询表达式的结果集中返回的行指定条件。搜索条件中可以包含的谓词数量没有限制。

**AND**

组合两个条件，当两个条件都为真时计算为真。

**OR**

组合两个条件，当任一条件为真时计算为真。

**< predicate >**

返回 TRUE 或 FALSE 的表达式。

**表达式**

是列名、常数、函数、变量、标量子查询，或由一个或多个运算符或子查询连接的列名、常数和函数的任意组合。表达式也可以包含 CASE 表达式。

**=**

用于测试两个表达式之间相等性的运算符。

**<>**

用于测试两个表达式不相等的条件的运算符。

**!=**

用于测试两个表达式不相等的条件的运算符。

**>**

用于测试一个表达式大于另一个表达式的条件的运算符。

**>=**

用于测试一个表达式大于或等于另一个表达式的条件的运算符。

**<**

用于测试一个表达式小于另一个表达式的条件的运算符。

**<=**

用于测试一个表达式小于或等于另一个表达式的条件的运算符。

**[NOT] IN**

用于测试一个表达式是否属于另一个表达式的条件的运算符。
使用方法支持以下两种

```sql
  expression [NOT] IN (expression2,...n)
```
*注意*： 支持同时设置多个表达式， 但用户须确保每个表达式返回值为单一值

```sql
  expression [NOT] IN expression2
```

*注意*： 用户须确保 expression2 的返回值为数组

```sql
SELECT column1, column2, ...
FROM table_name
WHERE condition;
```



## GROUP BY

GROUP BY 将一组选定的行分组为一组汇总行，这些汇总行按一个或多个列或表达式的值分组。

### 句法

```sql
GROUP BY <group by spec>  
  
<group by spec> ::=  
    <group by item> [ ,...n ]  
    | <window_type>  
  
<group by item> ::=  
    <column_expression>  
```

### 参数

**<window_type>**

指定任何支持eKuiper的窗口，有关详细信息，请参阅 [windows](windows.md) 。

**< column_expression >**

执行分组操作的列的表达式或名称。列表达式不能包含在选择列表中定义的列别名。

```sql
SELECT column_name(s)
FROM stream1
GROUP BY column_name
```

### HAVING

指定组或集合的搜索条件。 HAVING 只能与 SELECT 表达式一起使用。 HAVING 通常在 GROUP BY 子句中使用。 如果不使用 GROUP BY，则 HAVING 的行为类似于WHERE 子句。

#### 句法

```sql
[ HAVING <search condition> ]  
```

#### 参数

**< search_condition >**

指定要满足的组或集合的搜索条件。

```sql
SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" GROUP BY name HAVING count(name) > 3
```

## ORDER BY

按一列或多列的值对行进行排序。

### 句法

```sql
ORDER BY column1, column2, ... ASC|DESC
```

sql 中的 ORDER BY 语句用于根据一个或多个列对获取的数据进行升序或降序排序。

- 默认情况下，ORDER BY 以**升序对数据进行排序。**
- 关键字 DESC 用于按降序排序数据，关键字 ASC 用于按升序排序。

### 参数

**ASC**

按升序对数据进行排序。

**DESC**

按降对数据进行排序。



```sql
SELECT column1, column2, ...
FROM table_name
ORDER BY column1, column2, ... ASC|DESC;
```

## Case Expression

The case expression evaluates a list of conditions and returns one of multiple possible result expressions. It let you use IF ... THEN ... ELSE logic in SQL statements without having to invoke procedures.

There are two types of case expression: simple case expression and searched case expression.

### Simple Case Expression

The simple case expression compares an expression to a set of simple expressions to determine the result.

#### Syntax

```sql
CASE value   
     WHEN conditionValue THEN result_expression [ ...n ]   
     [ ELSE else_result_expression ]   
END   
```

**Example**:

```sql
SELECT CASE color 
    WHEN "red" THEN 1 
    WHEN "yellow" THEN 2 
    ELSE 3 END as colorInteger, 
humidity FROM tbl
```

### Searched Case Expression

The searched case expression evaluates a set of bool expressions to determine the result.

#### Syntax

```sql
CASE    
     WHEN condition THEN result_expression [ ...n ]   
     [ ELSE else_result_expression ]   
END 
```

**Example**:

```sql
SELECT CASE 
    WHEN size < 150 THEN "S" 
    WHEN size < 170 THEN "M"
    WHEN size < 175 THEN "L"
    ELSE "XL" END as sizeLabel
FROM tbl
```

## 使用保留字或特殊字符
如果你想在 SQL 或者流管理中使用保留关键字，或者特殊字符，请参考 [eKuiper 词法元素](lexical_elements.md).

