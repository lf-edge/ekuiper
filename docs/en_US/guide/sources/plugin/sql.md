# Sql Source

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>
<span style="background:green;color:white;padding:1px;margin:2px">lookup table source</span>

The source will query the database periodically to get data stream.

## Compile & deploy plugin

This plugin must be used in conjunction with at least a database driver. We are using build tag to determine which driver will be included.
This [repository](https://github.com/lf-edge/ekuiper/tree/master/extensions/sqldatabase/driver) lists all the supported drivers.

This plugin supports `sqlserver\postgres\mysql\sqlite3\oracle` drivers by default. User can compile plugin that only support one driver by himself,
for example, if he only wants sqlserver, then he can build with build tag `sqlserver`.

When using `sqlserver` as the target, you need to confirm that the `sqlserver` exposes port 1434.

### Default build command

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sources/Sql.so extensions/sources/sql/*.go
# cp plugins/sources/Sql.so $eKuiper_install/plugins/sources
```

### Sqlserver build command

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -tags sqlserver -o plugins/sources/Sql.so extensions/sources/sql/*.go
# cp plugins/sources/Sql.so $eKuiper_install/plugins/sources
```

Restart the eKuiper server to activate the plugin.

## Configuration

The configuration for this source is `$ekuiper/etc/sources/sql.yaml`. The format is as below:

```yaml
default:
  interval: 10000
  url: mysql://user:test@140.210.204.147/user?parseTime=true
  internalSqlQueryCfg:
    table: test
    limit: 1
    indexField: registerTime
    indexValue: "2022-04-21 10:23:55"
    indexFieldType: "DATETIME"
    dateTimeFormat: "YYYY-MM-dd HH:mm:ss"

sqlserver_config:
  url: sqlserver://username:password@140.210.204.147/testdb
  internalSqlQueryCfg:
    table: Student
    limit: 10
    indexField: id
    indexValue: 1000

template_config:
  templateSqlQueryCfg:
    TemplateSql: "select * from table where entry_data > {{.entry_data}}"
    indexField: entry_data
    indexValue: "2022-04-13 06:22:32.233"
    indexFieldType: "DATETIME"
    dateTimeFormat: "YYYY-MM-dd HH:mm:ssSSS"
```

### Global configurations

User can specify the global sql source settings here. The configuration items specified in `default` section will be taken as default settings for the source when running this source.

### interval

The interval (ms) to issue a query.

### url

The target database url

| database   | url sample                                            |
| ---------- | ----------------------------------------------------- |
| mysql      | mysql://user:test@140.210.204.147/user?parseTime=true |
| sql server | sqlserver://username:password@140.210.204.147/testdb  |
| postgres   | postgres://user:pass@localhost/dbname                 |
| sqlite     | sqlite:/path/to/file.db                               |

### internalSqlQueryCfg

* `table`: table name to query
* `limit`: how many items need fetch from the result
* `indexField`: which column for the table act as index to record the offset
* `indexValue`: initial index value, if user specify this field, the query will use this initial value as query condition, will update next query when get a greater value.
* `indexFieldType`: column type for the indexField, if it is dateTime type, must set this field with `DATETIME`
* `dateTimeFormat`: data time format for the index field
* `indexFields`: multiple index for the table

| table   | limit | indexField   | indexValue            | indexFieldType | dateTimeFormat        | sql query statement                                                                                 |
| ------- | ----- | ------------ | --------------------- | -------------- | --------------------- | --------------------------------------------------------------------------------------------------- |
| Student | 10    |              |                       |                |                       | select * from Student limit 10                                                                      |
| Student | 10    | stun         | 100                   |                |                       | select * from Student where stun > 100 limit 10                                                     |
| Student | 10    | registerTime | "2022-04-21 10:23:55" | "DATETIME"     | "YYYY-MM-dd HH:mm:ss" | select * from Student where registerTime > '2022-04-21 10:23:55' order by registerTime ASC limit 10 |

```yaml
internalSqlQueryCfg:
  # table name to query
  table: t
  # how many items need fetch from the result
  limit: 1
  indexFields:
      # which column for the table act as index to record the offset
    - indexField: a
      # initial index value, if user specify this field, the query will use this initial value as query condition, will update next query when get a greater value.
      indexValue: "2022-04-21 10:23:55"
      # column type for the indexField, if it is dateTime type, must set this field with `DATETIME`
      indexFieldType: "DATETIME"
      #  data time format for the index field
      dateTimeFormat: "YYYY-MM-dd HH:mm:ss"
    - indexField: b
      indexValue: 1
```

For indexFields, eKuiper will generate corresponding query statements for all index columns. It is worth noting that for query statements with multiple index columns, the declaration order of indexFields will determine the priority of index column sorting. For the above example, it will generate The following SQL:

```sql
select * from t where a > '2022-04-21 10:23:55' and b > 1 order by a asc, b asc limit 1
```

### templateSqlQueryCfg

* `TemplateSql`: sql statement template
* `indexField`: which column for the table act as index to record the offset
* `indexValue`: initial index value, if user specify this field, the query will use this initial value as query condition, will update next query when get a greater value.
* `indexFieldType`: column type for the indexField, if it is dateTime type, must set this field with `DATETIME`
* `dateTimeFormat`: data time format for the index field
* `indexFields`: multiple index for the table

::: v-pre

| TemplateSql                                                                                       | indexField   | indexValue            | indexFieldType | dateTimeFormat        | sql query statement                                                                                 |
| ------------------------------------------------------------------------------------------------- | ------------ | --------------------- | -------------- | --------------------- | --------------------------------------------------------------------------------------------------- |
| select * from Student limit 10                                                                    |              |                       |                |                       | select * from Student limit 10                                                                      |
| select * from Student where stun > {{.stun}} limit 10                                             | stun         | 100                   |                |                       | select * from Student where stun > 100 limit 10                                                     |
| select * from Student where registerTime > '{{.registerTime}}' order by registerTime ASC limit 10 | registerTime | "2022-04-21 10:23:55" | "DATETIME"     | "YYYY-MM-dd HH:mm:ss" | select * from Student where registerTime > '2022-04-21 10:23:55' order by registerTime ASC limit 10 |

:::

### *Note*: users only need set internalSqlQueryCfg or templateSqlQueryCfg, if both set, templateSqlQueryCfg will be used

You can check the connectivity of the corresponding sink endpoint in advance through the API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check)

## Override the default settings

If you have a specific connection that need to overwrite the default settings, you can create a customized section. In the previous sample, we create a specific setting named with `template_config`.  Then you can specify the configuration with option `CONF_KEY` when creating the stream definition (see [stream specs](../../../sqls/streams.md) for more info).

## Sample usage

```text
demo (
  ...
 ) WITH (DATASOURCE="demo", FORMAT="JSON", CONF_KEY="template_config", TYPE="sql");
```

The configuration keys "template_config" will be used.

## Lookup Table

The SQL source supports to be a lookup table. We can use create table statement to create a SQL lookup table. It will bind to the physical SQL DB and query on demand.

```text
CREATE TABLE alertTable() WITH (DATASOURCE="tableName", CONF_KEY="sqlite_config", TYPE="sql", KIND="lookup")
```

### Lookup cache

Query external DB is supposed to be slower than in memory calculation. If the throughput is high, the lookup cache can be used to improve the performance.

If lookup cache is not enabled, so all the requests are sent to external database. When lookup cache is enabled, each lookup table instance will hold a cache. When querying, we will first look up the cache before sending to the external database.

The cache configuration lies in the `sql.yaml`.

```yaml
  lookup:
    cache: true
    cacheTtl: 600
    cacheMissingKey: true
```

* cache: bool value to indicate whether to enable cache.
* cacheTtl: the time to live of the cache in seconds.
* cacheMissingKey: whether to cache nil value for a key.
