# Stream specs 

## Data types

In eKuiper, each column or an expression has a related data type. A data type describes (and constrains) the set of values that a column of that type can hold or an expression of that type can produce.

Below is the list of data types supported.

| #   | Data type | Description                                                                                                               |
|-----|-----------|---------------------------------------------------------------------------------------------------------------------------|
| 1   | bigint    |                                                                                                                           |
| 2   | float     |                                                                                                                           |
| 3   | string    |                                                                                                                           |
| 4   | datetime  |                                                                                                                           |
| 5   | boolean   |                                                                                                                           |
| 6   | bytea     | A sequence of bytes to store binary data. If the stream format is "JSON", the bytea field must be a base64 encoded string |
| 7   | array     | The array type, can be any simple types or array and type.                                                                |
| 8   | struct    | The complex type.                                                                                                         |

## Language definitions

```sql
CREATE STREAM   
    stream_name   
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

**The supported property names.**

| Property name    | Optional | Description                                                                                                                                                                                                                        |
|------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DATASOURCE       | false    | The value is determined by source type. The topic names list if it's a MQTT data source. Please refer to related document for other sources.                                                                                       |
| FORMAT           | true     | The data format, currently the value can be "JSON", "PROTOBUF" and "BINARY". The default is "JSON". Check [Binary Stream](#binary-stream) for more detail.                                                                         |
| SCHEMAID         | true     | The schema to be used when decoding the events. Currently, only use when format is PROTOBUF.                                                                                                                                       |
| DELIMITER        | true     | Only effective when using `delimited` format, specify the delimiter character, default is commas.                                                                                                                                  |
| KEY              | true     | Reserved key, currently the field is not used. It will be used for GROUP BY statements.                                                                                                                                            |
| TYPE             | true     | The source type, if not specified, the value is "mqtt".                                                                                                                                                                            |
| StrictValidation | true     | To control validation behavior of message field against stream schema. See [Strict Validation](#strict-validation) for more info.                                                                                                  |
| CONF_KEY         | true     | If additional configuration items are requied to be configured, then specify the config key here. See [MQTT stream](../guide/sources/builtin/mqtt.md) for more info.                                                               |
| SHARED           | true     | Whether the source instance will be shared across all rules using this stream                                                                                                                                                      |
| TIMESTAMP        | true     | The field to represent the event's timestamp. If specified, the rule will run with event time. Otherwise, it will run with processing time. Please refer to [timestamp management](./windows.md#timestamp-management) for details. |
| TIMESTAMP_FORMAT | true     | The default format to be used when converting string to or from datetime type.                                                                                                                                                     |

**Example 1,**

```sql
my_stream 
  (id bigint, name string, score float)
WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");
```

The stream will subscribe to MQTT topic ``topic/temperature``, the server connection uses ``server`` key of ``default`` section in configuration file ``$ekuiper/etc/mqtt_source.yaml``. 

- See [MQTT source](../guide/sources/builtin/mqtt.md) for more info.

**Example 2,**

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

The stream will subscribe to MQTT topic `test/`, the server connection uses settings of `demo` section in configuration file `$ekuiper/etc/mqtt_source.yaml`. 

**Example 3**

```sql
demo () WITH (DATASOURCE="test/", FORMAT="protobuf", SCHEMAID="proto1.Book");
```

The stream will subscribe to MQTT topic `test/` and using PROTOBUF format to decode the data. The decode schema is defined by `BOOK` message type in `$ekuiper/data/schemas/protobuf/schema1.proto` file. Regardng the management of schema, please refer to [schema registry](../guide/streams/codecs.md#schema).

- See [MQTT source](../guide/sources/builtin/mqtt.md) for more info.

- See [rules and streams CLI docs](../api/cli/overview.md) for more information of rules & streams management.

### Share source instance across rules

By default, each rule will instantiate its own source instance. In some scenarios, users may need to manipulate the exact same data stream with different rules. For example, for the data of temperature from a sensor. They may want to trigger an alert when the average for a period of time is higher than 30 degree and trigger another alert when it is lower than 0. With default configuration, each rule creates a source instance and may receive data in different order due to network delay or other factors so that the average calculation may happen with different context. By sharing the instance, we can assure both rules are processing the same data. Additionally, it will have better performance by eliminating the overhead of instantiation.

To use the share instance mode, just set the `SHARED` option to true in the stream definition. 

```
demo (
		...
	) WITH (DATASOURCE="test", FORMAT="JSON", KEY="USERID", SHARED="true");
```

## Schema

The schema of a stream contains two parts. One is the data structure defined in the data source definition, i.e. the logical schema, and the other is the SchemaId specified when using strongly typed data formats, i.e. the physical schema, such as those defined in Protobuf and Custom formats.

Overall, we will support 3 recursive ways of schema.

1. Schemaless, where the user does not need to define any kind of schema, mainly used for weakly structured data flows, or where the data structure changes frequently.
2. Logical schema only, where the user defines the schema at the source level, mostly used for weakly typed encoding, such as the JSON format, for users whose data has a fixed or roughly fixed format and do not want to use a strongly typed data codec format. In the case, the StrictValidation parameter can be used to configure whether to perform data validation and conversion. 
3. Physical schema, the user uses protobuf or custom formats and defines the schemaId, where the validation of the data structure is done by the format implementation.

Both the logical and physical schema definitions are used for SQL syntax validation in the parsing and loading phases of rule creation and for runtime optimization. The inferred schema of the stream can be obtained via [Schema API](../api/restapi/streams.md#get-stream-schema).


### Strict Validation

Used only for logically schema streams. If strict validation is set, the rule will verify the existence of the field and validate the field type based on the schema. If the data is in good format, it is recommended to turn off validation.

### Schema-less stream
If the data type of the stream is unknown or varying, we can define it without the fields. This is called schema-less. It is defined by leaving the fields empty.
```sql
schemaless_stream 
  ()
WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");
```

Schema-less stream field data type will be determined at runtime. If the field is used in an incompatible clause, a runtime error will be thrown and send to the sink. For example, `where temperature > 30`. Once a temperature is not a number, an error will be sent to the sink.

See [Query languange element](query_language_elements.md) for more inforamtion of SQL language.

### Binary Stream

Specify "BINARY" format for streams of binary data such as image or video streams. The payload of such streams is a block of binary data without fields. So it is required to define the stream as only one field of `bytea`. In the below example, the payload will be parsed into `image` field of `demoBin` stream.

```sql
demoBin (
	image BYTEA
) WITH (DATASOURCE="test/", FORMAT="BINARY");
```

If "BINARY" format stream is defined as schemaless, a default field named `self` will be assigned for the binary payload.
