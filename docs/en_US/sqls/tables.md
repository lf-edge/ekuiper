# TABLE Statements

SQL statements are defined to create and manage tables.

## Create TABLE

`CREATE TABLE` defines a table that is persisted in durable storage and can be joined with streams.

```sql
CREATE TABLE
    table_name
( column_name <data_type> [ ,...n ] )
WITH ( property_name = expression [, ...] );
```

For the detail table spec, please check [table](../guide/tables/overview.md).

## Describe Table

A statement to get the table definition.

```SQL
DESCRIBE TABLE table_name
```

## Drop Table

Delete a table. Please make sure all the rules which refer to the table are deleted.

```SQL
DROP TABLE stream_name
```

## Show Tables

Display all the tables defined.

```SQL
SHOW TABLES
```