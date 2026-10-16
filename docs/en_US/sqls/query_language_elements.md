
# Query language elements

eKuiper provides a variety of elements for building queries. They are summarized below.

| Element               | Summary                                                                                                                                                                                                                                       |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [SELECT](#select)     | SELECT is used to retrieve rows from input streams and enables the selection of one or many columns from one or many input streams in eKuiper.                                                                                                |
| [FROM](#from)         | FROM specifies the input stream. The FROM clause is always required for any SELECT statement.                                                                                                                                                 |
| [JOIN](#join)         | JOIN is used to combine records from two or more input streams. JOIN includes LEFT, RIGHT, FULL & CROSS. Join can apply to multiple streams join or stream/table join. To join multiple streams, it must run within a [window](./windows.md). |
| [WHERE](#where)       | WHERE specifies the search condition for the rows returned by the query.                                                                                                                                                                      |
| [GROUP BY](#group-by) | GROUP BY groups a selected set of rows into a set of summary rows grouped by the values of one or more columns or expressions. It must run within a [window](./windows.md).                                                                   |
| [ORDER BY](#order-by) | Order the rows by values of one or more columns.                                                                                                                                                                                              |
| [HAVING](#having)     | HAVING specifies a search condition for a group or an aggregate. HAVING can be used only with the SELECT expression.                                                                                                                          |
| [LIMIT](#limit) | LIMIT will limit the number of output data. |
| [UNION](#union) | Combines the rows of two streams and removes duplicates. |
| [UNION ALL](#union-all) | Combines the rows of two streams and keeps duplicates. |
| [INTERSECT](#intersect) | Returns the distinct rows of the left stream that also appear in the right one. |
| [INTERSECT ALL](#intersect-all) | Like INTERSECT, but keeps duplicates. |
| [EXCEPT](#except) | Returns the distinct rows of the left stream that don't appear in the right one. |
| [EXCEPT ALL](#except-all) | Like EXCEPT, but keeps duplicates. |

## SELECT

Retrieves rows from input streams and enables the selection of one or many columns from one or many input streams in eKuiper.

### Syntax

```sql
SELECT
    * [EXCEPT | REPLACE]
    | [source_stream.]column_name [AS column_alias]
    | expression

```

### Arguments

Specifies that all columns from all input streams in the FROM clause should be returned. The columns are returned by input source, as specified in the FROM clause, and in the order in which they exist in the incoming stream or specified by ORDER BY clause.

**\***

Select all of fields from source stream.

example:

```sql
select * from demo;
```

**\* EXCEPT**

Specify one or more fields to be excluded from the result. It allows excluding one or more specific column names from the query result while still including other columns.

```sql
SELECT * EXCEPT(column_name1, column_name2...)
FROM stream1
```

example:

```sql
select * except(a,b) from demo;
```

**\* REPLACE**

Replace specific columns in the result. It allows for the replacement of certain columns in the result by specifying new expressions, while other columns are still included in the output.

```sql
SELECT * REPLACE(expression1 as column_name1, expression2 as column_name2...)
FROM stream1
```

example:

```sql
select * replace(a+b as c) from demo;
```

REPLACE and EXCEPT can be used together, but it's important to note that if there is a conflict between these two operations, REPLACE takes precedence. In the following example, the final result will include the column_name1 field.

```sql
SELECT * EXCEPT(column_name1, column_name2) REPLACE(expression1 as column_name1, expression3 as column_name3)
FROM stream1
```

**source_stream**

The source stream name or alias name.

**column_name**

Is the name of a column to return.  If the column to specified is a embedded nest record type, then use the [JSON expressions](json_expr.md) to refer the embedded columns.

**column_alias**

Is an alternative name to replace the column name in the query result set.  Aliases are used also to specify names for the results of expressions. column_alias cannot be used in a WHERE, GROUP BY, or HAVING clause.

column_alias can participate in the calculation of select field, the following is an example:

```sql
select a + 1 as sum1, sum1 + 1 as sum2 from demo
```

When a is 1, the result is as follows:

```sql
{"sum1":2, "sum2":3}
```

It is worth noting that when the name of an alias has the same name as the column being defined, the name will be parsed as a column in the select field where the alias is located, and it will be parsed as an alais name in other select fields. Here is an example:

```sql
select a + 1 as a, a + 2 as sum2 from demo
```

When a is 1, the result is as follows:

```sql
{"a":2, "sum2":4}
```

**expression**

Expression is a constant, function, any combination of column names, constants, and functions connected by an operator or operators.

## FROM

Specifies the input stream. The FROM clause is always required for any SELECT statement.

### Syntax

```sql
FROM source_stream | source_stream AS source_stream_alias
```

### Arguments

**source_stream | source_stream_alias**

The input stream name or alias name.

## JOIN

JOIN is used to combine records from two or more input streams. JOIN includes LEFT, RIGHT, FULL & CROSS.

### Syntax

```sql
LEFT | RIGHT | FULL | CROSS
JOIN
source_stream | source_stream AS source_stream_alias
ON <source_stream|source_stream_alias>.column_name =<source_stream|source_stream_alias>.column_name
```

### Arguments

**LEFT**

The LEFT JOIN keyword returns all records from the left stream (stream1), and the matched records from the right stream (stream2). The result is NULL from the right side, if there is no match.

```sql
SELECT column_name(s)
FROM stream1
LEFT JOIN stream2
ON stream1.column_name = stream2.column_name;
```

example:

```sql
select * from stream1 left join stream2 on stream1.column = stream2.column group by countwindow(5);
```

**RIGHT**

The RIGHT JOIN keyword returns all records from the right stream (stream2), and the matched records from the left stream (stream1). The result is NULL from the left side, when there is no match.

```sql
SELECT column_name(s)
FROM stream1
RIGHT JOIN stream2
ON stream1.column_name = stream2.column_name;
```

example:

```sql
select * from stream1 right join stream2 on stream1.column = stream2.column group by countwindow(5);
```

**FULL**

The FULL JOIN keyword return all records when there is a match in left (stream1) or right (stream2) table records.

**Note:** FULL JOIN can potentially return large result-sets!

```sql
SELECT column_name(s)
FROM stream1
FULL JOIN stream2
ON stream1.column_name = stream2.column_name
WHERE condition;
```

example:

```sql
select * from stream1 full join stream2 on stream1.column = stream2.column group by countwindow(5);
```

**CROSS**

The CROSS JOIN is used to combine each row of the first stream (stream1) with each row of the second stream (stream2). It is also known as the Cartesian join since it returns the Cartesian product of the sets of rows from the joined tables. Let's say if there are **m** rows in stream1, and **n** rows in stream2, then the result of CROSS  JOIN returns **m*n** rows.

**Note:** CROSS JOIN can potentially return very large result-sets!

```sql
SELECT column_name(s)
FROM stream1
CROSS OUTER JOIN stream2
ON stream1.column_name = stream2.column_name
WHERE condition;
```

example:

```sql
select * from stream1 cross outer join on stream2 stream1.column = stream2.column group by countwindow(5);
```

**source_stream | source_stream_alias**

The input stream name or alias name to be joined.

**column_name**

Is the name of a column to return.  If the column to specified is a embedded nest record type, then use the [JSON expressions](json_expr.md) to refer the embedded columns.

## WHERE

WHERE specifies the search condition for the rows returned by the query. The WHERE clause is used to extract only those records that fulfill a specified condition.

### Syntax

```text
WHERE <search_condition>
<search_condition> ::= 
    { <predicate> | ( <search_condition> ) } 
    [ { AND | OR } { <predicate> | ( <search_condition> ) } ] 
[ ,...n ] 
<predicate> ::= 
    { expression { = | < > | ! = | > | > = | < | < = } expression 
```

exmaple:

```sql
select * from demo where a > 10;
```

### Arguments

Expression is a constant, function, any combination of column names, constants, and functions connected by an operator or operators.

**< search_condition >**

Specifies the conditions for the rows returned in the result set for a SELECT statement or query expression. There is no limit to the number of predicates that can be included in a search condition.

**AND**

Combines two conditions and evaluates to TRUE when both of the conditions are TRUE.

example:

```sql
select * from demo where a > 10 and a < 15;
```

**OR**

Combines two conditions and evaluates to TRUE when either condition is TRUE.

exmaple:

```sql
select * from demo where a > 10 or a < 15;
```

**< predicate >**

Is an expression that returns TRUE or FALSE.

**expression**

Is a column name, a constant, a function, a variable, a scalar subquery, or any combination of column names, constants, and functions connected by an operator or operators, or a subquery. The expression can also contain the CASE expression.

**=**

Is the operator used to test the equality between two expressions.

**<>**

Is the operator used to test the condition of two expressions not being equal to each other.

**!=**

Is the operator used to test the condition of two expressions not being equal to each other.

**>**

Is the operator used to test the condition of one expression being greater than the other.

**>=**

Is the operator used to test the condition of one expression being greater than or equal to the other expression.

**<**

Is the operator used to test the condition of one expression being less than the other.

**<=**

Is the operator used to test the condition of one expression being less than or equal to the other expression.

**[NOT] BETWEEN**

Is the operator used to test the condition of one expression in (not) within the range specified.

```sql
expression [NOT] BETWEEN expression1 AND expression2
```

exmaple:

```sql
select * from demo where a between 10 and 15;
```

**[NOT] LIKE**

Is the operator used to check if the STRING in the first operand matches a pattern specified by the second operand. Patterns can contain these characters:

- "%" matches any number of characters.
- "_" matches a single character.

```sql
expression [NOT] LIKE expression1
```

Example:

```sql
a LIKE "string%"
```

exmaple:

```sql
select * from demo where a like "prefix%"
```

**[NOT] IN**

Is the operator used to test the condition of one expression (not) being part of to the other expression. support these two formats

```sql
  expression [NOT] IN (expression2,...n)
```

*Note*： support multiple expressions at the same time, but each expression must return single value

```sql
  expression [NOT] IN expression2
```

*Note*：user must make sure the result of expression2 is in array format

```sql
SELECT column1, column2, ...
FROM table_name
WHERE condition;
```

## GROUP BY

GROUP BY groups a selected set of rows into a set of summary rows grouped by the values of one or more columns or expressions.

### Syntax

```sql
GROUP BY <group by spec>

<group by spec> ::=
    <group by item> [ ,...n ]
    | <window_type>

<group by item> ::=
    <column_expression>
```

## Arguments

**<window_type>**

Specifies any eKuiper supported Windowing, see [windows](windows.md) for more info.

**< column_expression >**

Is the expression or the name of the column on which the grouping operation is performed. The column expression cannot contain a column alias that is defined in the SELECT list.

```sql
SELECT column_name(s)
FROM stream1
GROUP BY column_name
```

example:

```sql
select * from demo group by a, countwindow(5);
```

### HAVING

The HAVING clause was added to SQL because the WHERE keyword could not be used with aggregate functions. Specifies a search condition for a group or an aggregate. HAVING can be used only with the SELECT expression. HAVING is typically used in a GROUP BY clause.

#### Syntax

```sql
[ HAVING <search condition> ]
```

#### Arguments

**< search_condition >**

Specifies the search condition for the group or the aggregate to meet.

```sql
SELECT temp AS t, name FROM topic/sensor1 WHERE name = "dname" GROUP BY name HAVING count(name) > 3
```

example:

```sql
select * from demo group by countwindow(5) having a > 10;
```

## ORDER BY

Order the rows by values of one or more columns.

### Syntax

```sql
ORDER BY column1, column2, ... ASC|DESC
```

The ORDER BY statement in sql is used to sort the fetched data in either ascending or descending according to one or more columns.

exmaple:

```sql
select * from demo group by countwindow(5) order by a ASC;
```

- By default ORDER BY sorts the data in **ascending order.**
- The keyword DESC is used to sort the data in descending order and the keyword ASC to sort in ascending order.

### Arguments

**ASC**

To sort the data in ascending order.

**DESC**

To sort the data in descending order.

```sql
SELECT column1, column2, ...
FROM table_name
ORDER BY column1, column2, ... ASC|DESC;
```

## LIMIT

Limit the number of output data

```sql
LIMIT 1
```

## Case Expression

The case expression evaluates a list of conditions and returns one of multiple possible result expressions. It let you use IF ... THEN ... ELSE logic in SQL statements without having to invoke procedures.

There are two types of case expression: simple case expression and searched case expression.

exmaple:

```sql
select * from demo where a > 10 group by countwindow(5) limit 10;
```

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

## Set operators

Set operators combine the results of two SELECT statements, each reading from its own stream:

```sql
SELECT <columns> FROM <left stream> [WHERE ...]
UNION | UNION ALL | INTERSECT | INTERSECT ALL | EXCEPT | EXCEPT ALL
SELECT <columns> FROM <right stream> [WHERE ...]
```

Streams never end, so the two statements are compared within a processing-time tumbling [window](./windows.md): the rows of both streams are collected for the length of the window, and the set operator then produces one result for that window. The length of the window is set by the rule options `setOperatorWindowLength` and `setOperatorWindowUnit`, which are required for rules with a set operator. See [rule options](../guide/rules/overview.md#fine-tuning) for details.

### UNION

UNION combines the rows of two streams and removes duplicates: the result contains every distinct row from the left stream and every distinct row from the right stream, each one only once.

**Example**:

```sql
SELECT name FROM demoStream
UNION
SELECT surname FROM mockStream
```

### UNION ALL

UNION ALL works like UNION, but keeps duplicates: the result contains every row from both streams.

**Example**:

```sql
SELECT name FROM demoStream
UNION ALL
SELECT surname FROM mockStream
```

### INTERSECT

INTERSECT returns the distinct rows of the left stream that also appear in the right stream.

**Example**:

```sql
SELECT name FROM demoStream
INTERSECT
SELECT surname FROM mockStream
```

### INTERSECT ALL

INTERSECT ALL works like INTERSECT, but keeps duplicates: if a row appears m times in the left stream and n times in the right stream, it appears min(m, n) times in the result.

**Example**:

```sql
SELECT name FROM demoStream
INTERSECT ALL
SELECT surname FROM mockStream
```

### EXCEPT

EXCEPT returns the distinct rows of the left stream that don't appear in the right stream.

**Example**:

```sql
SELECT name FROM demoStream
EXCEPT
SELECT surname FROM mockStream
```

### EXCEPT ALL

EXCEPT ALL works like EXCEPT, but keeps duplicates: if a row appears m times in the left stream and n times in the right stream, it appears m - n times in the result, or not at all if n is greater than or equal to m.

**Example**:

```sql
SELECT name FROM demoStream
EXCEPT ALL
SELECT surname FROM mockStream
```

### Matching columns and rows

The two SELECT statements must produce compatible rows:

- **Same number of columns.** Both statements must select the same number of columns. Fields marked as `INVISIBLE` are not part of the output, so they are not counted. For example, `SELECT device, ts INVISIBLE FROM roomA` has one column and can be combined with `SELECT device FROM roomB`.
- **Columns are matched by position, not by name.** The first visible column of the left statement is compared with the first visible column of the right statement, and so on. The names don't need to match: `SELECT device AS id FROM roomA UNION SELECT device FROM roomB` compares `id` with `device`.
- **Compatible types.** When the type of both columns is known, they must have the same type, except that `bigint` and `float` are compatible with each other. Otherwise the rule is rejected, for example: `incompatible types in column 1 of set operator: name is string but id is bigint`. A type is known only when the column is a field of a stream with a schema, directly or through an alias. Columns computed by functions or expressions, and fields of schemaless streams, are not checked.

Two rows are equal when all their visible columns are equal:

- Numbers are compared by value, so `12` equals `12.0`.
- NULL values are equal to each other.
- Structs and arrays are compared by their content.
- Datetime values are equal when they represent the same instant, in any time zone.

### Limitations

Set operators currently have these limitations. A rule that doesn't follow them is rejected when it is created.

- A rule can contain only one set operator, so it combines exactly two SELECT statements.
- The two statements must read from different streams.
- Each statement reads from a single stream: JOIN is not supported.
- Each statement can't use `*` (or `stream.*`), ORDER BY, LIMIT or GROUP BY.
- Functions that change the number of output columns or rows, such as `changed_cols` and `unnest`, are not supported.
- Aggregate functions are allowed, but there is no GROUP BY window inside the statements, so they are computed for each message, not over the set operator window. For example, `count(*)` is always 1.
- Event time is not supported: the rule option `isEventTime` must be false.
- The rule option `sendMetaToSink` is not supported.

## Use reserved keywords or special characters

If you'd like to use reserved keywords or special characters in rule SQL or streams management, please refer to [eKuiper lexical elements](lexical_elements.md).
