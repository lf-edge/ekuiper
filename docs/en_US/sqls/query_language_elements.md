
# Query language elements

Kuiper provides a variety of elements for building queries. They are summarized below.

| Element               | Summary                                                      |
| --------------------- | ------------------------------------------------------------ |
| [SELECT](#SELECT)     | SELECT is used to retrieve rows from input streams and enables the selection of one or many columns from one or many input streams in Kuiper. |
| [FROM](#FROM)         | FROM specifies the input stream. The FROM clause is always required for any SELECT statement. |
| [JOIN](#JOIN)         | JOIN is used to combine records from two or more input streams. JOIN includes LEFT, RIGHT, FULL & CROSS. |
| [WHERE](#WHERE)       | WHERE specifies the search condition for the rows returned by the query. |
| [GROUP BY](#GROUP BY) | GROUP BY groups a selected set of rows into a set of summary rows grouped by the values of one or more columns or expressions. |
| [ORDER BY](#ORDER BY) | Order the rows by values of one or more columns.             |
| [HAVING](#HAVING)     | HAVING specifies a search condition for a group or an aggregate. HAVING can be used only with the SELECT expression.             |
|                       |                                                              |
### Use reserved words
These elements and other predefined operators, functions are reserved words. To use the reserved words as the column name and the table name etc., they need to be quoted by backtick.

```tsql
SELECT `select`, `and` from demo
```


## SELECT

Retrieves rows from input streams and enables the selection of one or many columns from one or many input streams in Kuiper.

### Syntax

```sql
SELECT 
	*
	| [source_stream.]column_name [AS column_alias]
	| expression
  
```

### Arguments

Specifies that all columns from all input streams in the FROM clause should be returned. The columns are returned by input source, as specified in the FROM clause, and in the order in which they exist in the incoming stream or specified by ORDER BY clause.

**\***

Select all of fields from source stream.

**source_stream**

The source stream name or alias name.

**column_name**

Is the name of a column to return.  If the column to specified is a embedded nest record type, then use the [JSON expressions](json_expr.md) to refer the embedded columns. 

**column_alias**

Is an alternative name to replace the column name in the query result set.  Aliases are used also to specify names for the results of expressions. column_alias cannot be used in a WHERE, GROUP BY, or HAVING clause.

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

**RIGHT**

The RIGHT JOIN keyword returns all records from the right stream (stream2), and the matched records from the left stream (stream1). The result is NULL from the left side, when there is no match.

```sql
SELECT column_name(s)
FROM stream1
RIGHT JOIN stream2
ON stream1.column_name = stream2.column_name;
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

**source_stream | source_stream_alias**

The input stream name or alias name to be joined.

**column_name**

Is the name of a column to return.  If the column to specified is a embedded nest record type, then use the [JSON expressions](json_expr.md) to refer the embedded columns. 

## WHERE

WHERE specifies the search condition for the rows returned by the query. The WHERE clause is used to extract only those records that fulfill a specified condition.

### Syntax

```
WHERE <search_condition>
<search_condition> ::=   
    { <predicate> | ( <search_condition> ) }   
    [ { AND | OR } { <predicate> | ( <search_condition> ) } ]   
[ ,...n ]   
<predicate> ::=   
    { expression { = | < > | ! = | > | > = | < | < = } expression   
```

### Arguments

Expression is a constant, function, any combination of column names, constants, and functions connected by an operator or operators.

**< search_condition >**

Specifies the conditions for the rows returned in the result set for a SELECT statement or query expression. There is no limit to the number of predicates that can be included in a search condition.

**AND**

Combines two conditions and evaluates to TRUE when both of the conditions are TRUE.

**OR**

Combines two conditions and evaluates to TRUE when either condition is TRUE.

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

Specifies any Kuiper supported Windowing, see [windows](windows.md) for more info.

**< column_expression >**

Is the expression or the name of the column on which the grouping operation is performed. The column expression cannot contain a column alias that is defined in the SELECT list.

```sql
SELECT column_name(s)
FROM stream1
GROUP BY column_name
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

## ORDER BY

Order the rows by values of one or more columns. 

### Syntax

```sql
ORDER BY column1, column2, ... ASC|DESC
```

The ORDER BY statement in sql is used to sort the fetched data in either ascending or descending according to one or more columns.

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

