# Table

In eKuiper, a table is a snapshot of the source data. In contrast to the common static tables that represent batch data, eKuiper tables can change over time.

The source for table can be either bounded or unbounded. For bounded source table, the content of the table is static. For unbounded table, the content of the table is dynamic.

## Table Updates

Currently, the table update in eKuiper is append-only. Users can specify the properties to limit the table size to avoid too much memory consumption.

## Table Usages

Table cannot be used standalone in a rule. It is usually used to join with streams. It can be used to enrich stream data or as a switch for calculation.

## More Readings

- [Table Reference](../../sqls/tables.md)