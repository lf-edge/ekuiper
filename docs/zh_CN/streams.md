## 流规格 


### 数据类型

参考 [Azure IoT](https://docs.microsoft.com/en-us/stream-analytics-query/data-types-azure-stream-analytics), 将布尔类型强制转换为int。

| #    | 数据类型 | 说明                                                         |
| ---- | -------- | ------------------------------------------------------------ |
| 1    | bigint   |                                                              |
| 2    | float    |                                                              |
| 3    | string   |                                                              |
| 4    | datetime | 需要指定日期格式？ 例如“ yyyy-MM-dd”                         |
| 5    | boolean  |                                                              |
| 6    | array    | 数组类型可以是简单数据或结构类型中的任何类型（＃1-＃5和＃7）。 |
| 7    | struct   | The complex type.                                            |



### 语言定义

#### 创建流

```sql
CREATE STREAM   
    stream_name   
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

**支持的属性名称。**

| 属性名 | 可选  | 说明                                                |
| ------------- | -------- | ------------------------------------------------------------ |
| DATASOURCE | 否   | 如果是MQTT数据源，则列出主题名称。 |
| FORMAT        | 否   | json 或Avro.<br />目前，我们仅支持JSON类型 ? |
| KEY           | 是    | 将来将用于GROUP BY语句 ?? |
| TYPE     | 否   | 如果支持越来越多的源，将来是否需要？ 默认情况下，它将是MQTT类型。 |
| StrictValidation     | 否    | 根据流模式控制消息字段的验证行为。 |
| CONF_KEY | 否 | 如果需要配置其他配置项，请在此处指定配置键。<br />Kuiper当前建议使用yaml文件格式。 |

**StrictValidation介绍**

``` 
StrictValidation的值可以为true或false。
1）True：如果消息不符合流定义，则删除消息。
2）False：保留消息，但用默认的空值填充缺少的字段。

bigint: 0
float: 0.0
string: ""
datetime: ??
boolean: false
array: zero length array
struct: null value
```

示例 1,

```sql
CREATE STREAM demo (
		USERID BIGINT,
		FIRST_NAME STRING,
		LAST_NAME STRING,
		NICKNAMES ARRAY(STRING),
		Gender BOOLEAN,
		ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT),
	) WITH (DATASOURCE="topics:test/, demo/test", FORMAT="AVRO", KEY="USERID", CONF_KEY="democonf");
```



示例 2,

```sql
CREATE STREAM my_stream   
    (id int, name string, score float)
    WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");
```



MQTT源的配置指定以yaml格式，并且配置文件位置在$ kuiper / etc / mqtt_source.yaml处。 以下是文件格式。

```yaml
#Global MQTT configurations
QoS: 1
Share-subscription: true
Servers:
  - 
     tcp://127.0.0.1:1883
#TODO: Other global configurations


#Override the global configurations
demo: #Conf_key
  QoS: 0
  Servers:
    - 
      tls://10.211.55.6:1883


```

#### 删除流

删除流。

```
DROP STREAM my_stream
```

#### 描述流

打印流定义。

```
DESC STREAM my_stream

Fields
-----------
id     int
name   string
score  float

SOURCE: topic/temperature
Format: json
Key: id
```

#### 解释流

打印流的详细运行时信息。

```
EXPLAIN STREAM my_stream
```

#### 显示流

打印系统中所有已定义的流。

```
SHOW STREAMS

my_stream, iot_stream
```



### 一个简单的CLI

一个简单的命令行工具在`stream/cli/main.go`中实现。

#### 运行SQL来管理流

运行`cli stream`命令，在显示 `kuiper>`提示后，输入与流相关的sql语句，例如create，drop，description，explain和show stream语句以执行操作。

```bash
cli stream
kuiper > CREATE STREAM sname (count bigint) WITH (source="users", FORMAT="AVRO", KEY="USERID")
kuiper > DESCRIBE STREAM sname
...
```


#### 查询

```bash
cli query
kuiper > select USERID from demo;
...
```



### 实现

##### 如何保存流定义？

请参阅下面的内容，需要使用存储器来保存流定义。

![stream_storage](resources/stream_storage.png)

