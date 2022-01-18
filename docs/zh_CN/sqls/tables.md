# 表规格

eKuiper 流是无界且不可变的，任何新数据都会附加到当前流中进行处理。 **Table** 用于表示流的当前状态。它可以被认为是流的快照。用户可以使用 table 来保留一批数据进行处理。

在 eKuiper 中不允许单独使用表格。仅建议与流进行 join 操作。join 流时，表格将在新事件到来时不断更新。但是，只有到达流端的事件才会触发下游更新并产生连接输出。

## 语法定义

表支持与流几乎相同的语法。要创建表，请运行以下 SQL：
```sql
CREATE TABLE   
    table_name   
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

表支持与流相同的 [数据类型](./streams.md#data-types)。
表还支持所有[流的属性](./streams.md#language-definitions)。因此，表中也支持所有源类型。许多源不是批处理的，它们在任何给定时间点都有一个事件，这意味着表将始终只有一个事件。一个附加属性 `RETAIN_SIZE` 来指定表快照的大小，以便表可以保存任意数量的历史数据。

## 使用场景

通常，表格将与带有或不带有窗口的流连接。与流连接时，表数据不会影响下游更新数据，它被视为静态引用数据，尽管它可能会在内部更新。

### 查询表

表的典型用法是作为查找表。示例 SQL 将类似于：
```sql
CREATE TABLE table1 (
		id BIGINT,
		name STRING
	) WITH (DATASOURCE="lookup.json", FORMAT="JSON", TYPE="file");

SELECT * FROM demo INNER JOIN table1 on demo.id = table1.id
```

在这个例子中，创建了一个表 `table1` 来从文件 *lookup.json* 中读取 json 数据。然后在规则中，将 `table1` 与流 `demo` 连接起来，以便流可以从 id 中查找名称。

*lookup.json* 文件的内容应该是一个对象数组。下面是一个例子：
```json
[
  {
    "id": 1541152486013,
    "name": "name1"
  },
  {
    "id": 1541152487632,
    "name": "name2"
  },
  {
    "id": 1541152489252,
    "name": "name3"
  }
]
```

### 按历史状态过滤

在某些情况下，我们可能有一个用于数据的事件流和另一个作为控制信息的事件流。
```sql
CREATE TABLE stateTable (
		id BIGINT,
		triggered bool
	) WITH (DATASOURCE="myTopic", FORMAT="JSON", TYPE="mqtt");

SELECT * FROM demo LEFT JOIN stateTable on demo.id = stateTable.id WHERE triggered=true
```
在此示例中，创建了一个表 `stateTable` 来记录来自 mqtt 主题 *myTopic* 的触发器状态。在规则中，会根据当前触发状态来过滤 `demo` 流的数据。