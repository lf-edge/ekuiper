# 表语句

用于创建和管理表的语句

## 创建表

`CREATE TABLE` 定义了一个存储状态的表，并可与流连接。

```sql
CREATE TABLE
    table_name
( column_name <data_type> [ ,...n ] )
WITH ( property_name = expression [, ...] );
```

详细的表语法和属性，请查看 [表](../guide/tables/overview.md)。

## 描述表

用于获取表定义的语句。

```SQL
DESCRIBE TABLE table_name
```

## 删除表

删除一个表。请确保所有涉及该流的规则都被删除。

```SQL
DROP TABLE stream_name
```

## 显示表

展示所有定义的表。

```SQL
SHOW TABLES
```
