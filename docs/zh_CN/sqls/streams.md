# 流规格

## 数据类型



在Kuiper中，每个列或表达式都有一个相关的数据类型。 数据类型描述（约束）该类型的列可以容纳的一组值或该类型可以产生的表达式。

以下是支持的数据类型的列表。

| #    | 数据类型 | 说明                                                   |
| ---- | -------- | ------------------------------------------------------ |
| 1    | bigint   |                                                        |
| 2    | float    |                                                        |
| 3    | string   |                                                        |
| 4    | datetime | 不支持                                                 |
| 5    | boolean  |                                                        |
| 6    | array    | 数组类型可以是任何简单类型或结构类型（＃1-＃5和＃7）。 |
| 7    | struct   | The complex type.                                      |

## 语言定义

```sql
CREATE STREAM   
    stream_name   
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

**支持的属性名称**

| Property name | Optional | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| DATASOURCE | 否   | MQTT数据源主题名称列表。 |
| FORMAT        | 否       | JSON. |
| KEY           | true     | 保留键，当前未使用该字段。 它将用于GROUP BY语句。 |
| TYPE    | 否       | 数据格式，当前值只能是“ JSON”。 |
| StrictValidation     | 否   | 针对流模式控制消息字段的验证行为。 有关更多信息，请参见[StrictValidation](#StrictValidation) |
| CONF_KEY | 否 | 如果需要配置其他配置项，请在此处指定config键。 有关更多信息，请参见 [MQTT stream](../rules/sources/mqtt.md) 。 |

**示例1**

```sql
my_stream 
  (id bigint, name string, score float)
WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");
```

该流将订阅MQTT主题topic / temperature，服务器连接使用配置文件``$ kuiper / etc / mqtt_source.yaml''中默认部分的servers键。

- 有关更多信息，请参见 [MQTT source](../rules/sources/mqtt.md) 

**示例2**

```sql
demo (
		USERID BIGINT,
		FIRST_NAME STRING,
		LAST_NAME STRING,
		NICKNAMES ARRAY(STRING),
		Gender BOOLEAN,
		ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT),
	) WITH (DATASOURCE="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo");
```

 流将订阅MQTT主题test /，服务器连接使用配置文件$ kuiper / etc / mqtt_source.yaml中demo部分的设置。

- 有关更多信息，请参见 [MQTT source](../rules/sources/mqtt.md) 

- 有关规则和流管理的更多信息，请参见 [规则和流 CLI docs](../cli/overview.md) 

### StrictValidation

```
The value of StrictValidation can be true or false.
1) True: Drop the message if the message  is not satisfy with the stream definition.
2) False: Keep the message, but fill the missing field with default empty value.

bigint: 0
float: 0.0
string: ""
datetime: (NOT support yet)
boolean: false
array: zero length array
struct: null value
```

有关SQL语言的更多信息，请参见 [Query languange element](query_language_elements.md) 。

