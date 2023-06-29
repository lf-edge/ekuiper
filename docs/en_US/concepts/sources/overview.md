# Sources

Sources are used to read data from external systems. The source can be unbounded streaming data named stream or bounded batch data named table. When using the source in a rule, at least one of the sources must be a stream.

The source basically defines how to connect to an external resource and fetch data from the resource in a streaming way. After fetching the data, common tasks like decode and transform by schema can be done by setting properties.

## Define and Run

When defining a source stream or table, it actually creates the logical definition instead of a physical running data input. The logical definition can then be used in rule's SQL in the `from` clause. The source only starts to run when any of the rules refer to it has started.

By default, if multiple rules refer to the same source, each rule will have its own, standalone source instance from other rules so that the rules are totally separated. To boost performance when users want to process the same data across multiple rules, they can define the source as [shared](../../guide/streams/overview.md#share-source-instance-across-rules).

## Decode

Users can define the format to decode by setting `format` property. Currently, `json`,  `binary`, `protobuf`, and `delimited` formats are supported. And you can also use your own decoding methods by setting it to `custom`.

## Schema

Users can define the schema of the data source like a relational database table. Some data formats come with their own schema, such as the `protobuf` format. When creating a source, you can define `schemaId` to point to the data structure definition in the Schema Registry.

Where the definition in the schema registry is the physical schema and the data structure in the data source definition statement is the logical schema. If both are defined, the physical schema will override the logical schema. In this case, the validation and formatting of the data will be the responsibility of the defined format, e.g. `protobuf`. If only the logical schema is defined and `strictValidation` is set, the data will be validated and type converted according to the defined structure in the eKuiper runtime. If no validation is set, the logical schema is mainly used for SQL statement validation at compile and load time. If the input data is pre-processed clean data or if the data structure is unknown or variable, the user may not define the schema, thus also avoiding the overhead of data conversion.

## Stream & Table

The source defines the external system connection. When using the source with a rule, users can define them as a stream or table according to the processing mechanism. Check [stream](stream.md) and [table](table.md) for detail.

## More Readings

- [Source Reference](../../guide/sources/overview.md)
