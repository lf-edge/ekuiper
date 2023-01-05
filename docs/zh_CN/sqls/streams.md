# 流语句

用于创建和管理流的语句

## 创建流

`CREATE STREAM` 定义了一个连接到外部系统的数据流，以加载数据。

```sql
CREATE STREAM   
    stream_name   
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

详细的流语法和属性，请查看[流](../guide/streams/overview.md)。

示例:

```SQL
CREATE STREAM my_stream ()
WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id")
```

## 描述流

用于获取流定义的语句。

```SQL
DESCRIBE STREAM stream_name
```

## 删除流

删除一个流。请确保所有涉及该流的规则都被删除。

```SQL
DROP STREAM stream_name
```

## 显示流

展示所有定义的流。

```SQL
SHOW STREAMS
```
