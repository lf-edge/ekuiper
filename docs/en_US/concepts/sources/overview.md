# Sources

Sources are used to read data from external systems. The source can be unbounded streaming data named stream or bounded batch data named table. When using in a rule, at least one of  the source must be a stream.

The source basically defines how to connect to an external resource and fetch data from the resource in a streaming way. After fetching the data, common tasks like decode and transform by schema can be done by setting properties.

## Define and Run

When define a source stream or table, it actually creates the logical definition instead of a physical running data input. The logical definition can then be used in rule's SQL in the `from` clause. The source only starts to run when any of the rules refer to it has started.

By default, if multiple rules refer to the same source. Each rule will have its own, standalone source instance from other rules so that the rules are total separated. To boost performance when users want to process the same data across multiple rules, they can define the source as shared. Then the rules refer to the same shared source will share the same running source instance.

## Decode

Users can define the format to decode by setting `format` property. Currently, only `json` and `binary` format are supported. For other formats, customized source must be developed.

## Schema

Users can define the data schema like a common SQL table. In eKuiper runtime, the data will be validated and transformed according to the schema. To avoid conversion overhead if the data is fixed and clean or to consume unknown schema data, users can define schemaless source.

## Stream & Table

The source defines the external system connection. When using in a rule, users can define them as stream or table according to the processing mechanism. Check [stream](stream.md) and [table](table.md) for detail.

## More Readings

- [Source Reference](../../rules/sources/overview.md)



