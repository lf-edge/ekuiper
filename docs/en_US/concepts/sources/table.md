# Table

A table is a snapshot of the source data. We support two kinds of tables: scan table and lookup table.

- Scan table: Consume the stream data as changelog and update the table continuously. In contrast to the common static tables that represent batch data, scan tables can change over time. All stream sources like MQTT, neuron source can also be a scan table source. Scan table was supported since v1.2.0.
- Lookup table: an external table whose content is usually never read entirely but queried for individual values when necessary. We support to bind physical table as table and generate lookup command(e.g. a SQL on db) on demand. Notice that, not all source type can be a lookup table source, only sources like SQL source which has an external storage can be a lookup source. Lookup table was supported since v1.7.0.

## Scan table

The source for table can be either bounded or unbounded. For bounded source table, the content of the table is static. For unbounded table, the content of the table is dynamic. The content of the table are stored in memory.

Currently, the scan table update in eKuiper is append-only. Users can specify the properties to limit the table size to avoid too much memory consumption.

Scan table cannot be used standalone in a rule. It is usually used to join with streams. It can be used to enrich stream data or as a switch for calculation.

## Lookup Table

Lookup table do not store the table content in memory but refer to the external table. Apparently, only a few of sources is suitable as lookup table which requires the source itself is queryable. The supported sources are:

- Memory source: if a memory source is used as table type, we need to accumulate the data as a table in memory. It can serve as a intermediate to convert any stream into a lookup table.
- Redis source: Support to query by redis key.
- SQL source: This is the most typical lookup source. We can use SQL directly to query. 

Unlike scan tables, lookup table will run separately from rules. Thus, all rules that refer to a lookup table can actually query the same table content.

## More Readings

- [Table Reference](../../sqls/tables.md)