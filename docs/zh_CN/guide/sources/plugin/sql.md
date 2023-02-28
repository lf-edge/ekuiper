# Sql 源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>
<span style="background:green;color:white">lookup table source</span>

源将定期查询数据库以获取数据流。

## 编译和部署插件

此插件必须与至少一个数据库驱动程序一起使用。我们使用构建标签来确定将包含哪个驱动程序。[此处](https://github.com/lf-edge/ekuiper/tree/master/extensions/sqldatabase/driver)列出了所有支持的驱动程序。

该插件默认支持 `sqlserver\postgres\mysql\sqlite3\oracle` 驱动。用户可以自己编译只支持一个驱动的插件，例如如果他只想要sqlserver，那么他可以用 build tag sqlserver 构建。

### 默认构建命令

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sources/Sql.so extensions/sources/sql/*.go
# cp plugins/sources/Sql.so $eKuiper_install/plugins/sources
```

### Sqlserver 构建命令

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -tags sqlserver -o plugins/sources/Sql.so extensions/sources/sql/*.go
# cp plugins/sources/Sql.so $eKuiper_install/plugins/sources
```

重启 eKuiper 服务器以激活插件

## 配置

这个数据流的配置文件位于 `$ekuiper/etc/sources/sql.yaml`. 格式如下:

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

### 全局配置

用户可以在此处指定全局 sql 源设置。 `default` 部分中指定的配置项将在运行此源时作为源的默认设置。

### interval

发出查询的时间间隔（毫秒）

### url

目标数据库地址

| database   | url sample                                            |
| ---------- | ----------------------------------------------------- |
| mysql      | mysql://user:test@140.210.204.147/user?parseTime=true |
| sql server | sqlserver://username:password@140.210.204.147/testdb  |
| postgres   | postgres://user:pass@localhost/dbname                 |
| postgres   | postgres://user:pass@localhost/dbname                 |
| sqlite     | sqlite:/path/to/file.db                               |

### internalSqlQueryCfg

* `table`: 要查询的表名
* `limit`: 需要从结果中获取多少条目
* `indexField`: 表的哪一列作为索引来记录偏移量
* `indexValue`: 初始索引值，如果用户指定该字段，查询将使用这个初始值作为查询条件，当获得更大的值时将更新下一个查询
* `indexFieldType`: 索引字段的列类型，如果是dateTime类型，必须将该字段设置为`DATETIME`
* `dateTimeFormat`: 索引字段的时间格式

| table   | limit | indexField   | indexValue            | indexFieldType | dateTimeFormat        | sql query statement                                                                                 |
| ------- | ----- | ------------ | --------------------- | -------------- | --------------------- | --------------------------------------------------------------------------------------------------- |
| Student | 10    |              |                       |                |                       | select * from Student limit 10                                                                      |
| Student | 10    | stun         | 100                   |                |                       | select * from Student where stun > 100 limit 10                                                     |
| Student | 10    | registerTime | "2022-04-21 10:23:55" | "DATETIME"     | "YYYY-MM-dd HH:mm:ss" | select * from Student where registerTime > '2022-04-21 10:23:55' order by registerTime ASC limit 10 |

### templateSqlQueryCfg

* `TemplateSql`: sql语句模板
* `indexField`: 表的哪一列作为索引来记录偏移量
* `indexValue`: 同上
* `indexFieldType`: 同上
* `dateTimeFormat`: 同上

::: v-pre
| TemplateSql                                                                                       | indexField   | indexValue            | indexFieldType | dateTimeFormat        | sql query statement                                                                                 |
| ------------------------------------------------------------------------------------------------- | ------------ | --------------------- | -------------- | --------------------- | --------------------------------------------------------------------------------------------------- |
| select * from Student limit 10                                                                    |              |                       |                |                       | select * from Student limit 10                                                                      |
| select * from Student where stun > {{.stun}} limit 10                                             | stun         | 100                   |                |                       | select * from Student where stun > 100 limit 10                                                     |
| select * from Student where registerTime > '{{.registerTime}}' order by registerTime ASC limit 10 | registerTime | "2022-04-21 10:23:55" | "DATETIME"     | "YYYY-MM-dd HH:mm:ss" | select * from Student where registerTime > '2022-04-21 10:23:55' order by registerTime ASC limit 10 |
:::

### *注意*: 用户只需要设置 internalSqlQueryCfg 或 templateSqlQueryCfg，如果两者都设置，将使用 templateSqlQueryCfg

## 覆盖默认配置

如果您有需要覆盖默认设置的特定连接，您可以创建自定义部分。在前面的示例中，我们创建了一个名为 `template_config` 的特定设置。然后，您可以在创建流定义时使用选项 `CONF_KEY` 指定配置（有关更多信息，请参阅 [流规范](../../../sqls/streams.md)）。

## 使用样例

```
demo (
  ...
 ) WITH (DATASOURCE="demo", FORMAT="JSON", CONF_KEY="template_config", TYPE="sql");
```

将使用配置键 `template_config`

## 查询表

SQL 源支持成为一个查询表。我们可以使用创建表语句来创建一个 SQL 查询表。它将与实体关系数据库绑定并按需查询。

```text
CREATE TABLE alertTable() WITH (DATASOURCE="tableName", CONF_KEY="sqlite_config", TYPE="sql", KIND="lookup")
```

### 查询缓存

查询外部数据库比在内存中计算要慢。如果吞吐量很高，可以使用查找缓存来提高性能。如果不启用查找缓存，那么所有的请求都被发送到外部数据库。当启用查找缓存时，每个查找表实例将持有一个缓存。当查询时，我们将首先查询缓存，然后再发送到外部数据库。

缓存的配置在`sql.yaml`中。

```yaml
  lookup:
    cache: true
    cacheTtl: 600
    cacheMissingKey: true
```

- cache: bool值，表示是否启用缓存。
- cacheTtl: 缓存的生存时间，单位是秒。
- cacheMissingKey：是否对空值进行缓存。