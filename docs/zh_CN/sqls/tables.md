# 表规格

eKuiper 流是无界且不可变的，任何新数据都会附加到当前流中进行处理。 **Table** 用于表示流的当前状态。它可以被认为是流的快照。用户可以使用 table 来保留一批数据进行处理。

有两种类型的表。

- 扫描表（Scan Table）：在内存中积累数据。它适用于较小的数据集，表的内容不需要在规则之间共享。
- 查询表（Lookup Table）：绑定外部表并按需查询。它适用于更大的数据集，并且在规则之间共享表的内容。

## 语法定义

表支持与流几乎相同的语法。要创建表，请运行以下 SQL：
```sql
CREATE TABLE   
    table_name   
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

表支持与流相同的 [数据类型](./streams.md#数据类型)。
表还支持所有[流的属性](./streams.md#语言定义)。因此，表中也支持所有源类型。许多源不是批处理的，它们在任何给定时间点都有一个事件，这意味着表将始终只有一个事件。一个附加属性 `RETAIN_SIZE` 来指定表快照的大小，以便表可以保存任意数量的历史数据。

### 查询表的语法

语法与创建普通扫描表相同，只需指定 `KIND` 的属性为 `lookup`。下面是一个创建查询表的例子，它会与 redis 的数据库 0 绑定。

```sql
CREATE TABLE alertTable() WITH (DATASOURCE="0", TYPE="redis", KIND="lookup")
```

目前，只有 `memory`、`redis` 和 `sql` 源可以作为查找表。

### 表的属性

| 属性名称       | 可选   | 描述                                                                                                                                                                      |
|------------|------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DATASOURCE | 否    | 取决于不同的源类型；如果是 MQTT 源，则为 MQTT 数据源主题名；其它源请参考相关的文档。                                                                                                                        |
| FORMAT     | 是    | 传入的数据类型，支持 "JSON", "PROTOBUF" 和 "BINARY"，默认为 "JSON" 。关于 "BINARY" 类型的更多信息，请参阅 [Binary Stream](#二进制流)。该属性是否生效取决于源的类型，某些源自身解析的时固定私有格式的数据，则该配置不起作用。可支持该属性的源包括 MQTT 和 ZMQ 等。 |
| SCHEMAID   | 是    | 解码时使用的模式，目前仅在格式为 PROTOBUF 的情况下使用。                                                                                                                                       |
| KEY        | true | 表的主键。例如，对于SQL源，用于指定SQL表中的主键。并非所有的源类型都支持该属性。                                                                                                                             |
| TYPE       | true | 源类型。每个源类型可以支持一种或两种表。请参考相关文件。                                                                                                                                            |
| CONF_KEY   | 是    | 如果需要配置其他配置项，请在此处指定 config 键。 有关更多信息，请参见 [MQTT stream](../rules/sources/builtin/mqtt.md) 。                                                                               |
| KIND       | true | 表的种类，可以是 `scan` 或 `lookup`。如果没有指定，默认值是`scan`。                                                                                                                           |


## 使用场景

通常，表格将与带有或不带有窗口的流连接。与流连接时，表数据不会影响下游更新数据，它被视为静态引用数据，尽管它可能会在内部更新。

### 数据补全

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