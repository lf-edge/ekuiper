# Sql 源

源将定期查询数据库以获取数据流。

## 编译和部署插件

此插件必须与至少一个数据库驱动程序一起使用。我们使用构建标签来确定将包含哪个驱动程序。[此处](https://github.com/lf-edge/ekuiper/tree/master/extensions/sqldatabase/driver)列出了所有支持的驱动程序。

该插件默认支持 `sqlserver\postgres\mysql\sqlite3\oracle` 驱动。用户可以自己编译只支持一个驱动的插件，例如如果他只想要sqlserver，那么他可以用 build tag sqlserver 构建。

### 默认构建命令

```shell
# cd $eKuiper_src
# go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/sources/Sql.so extensions/sources/sql/sql.go
# cp plugins/sources/Sql.so $eKuiper_install/plugins/sources
```

### Sqlserver 构建命令

```shell
# cd $eKuiper_src
# go build -trimpath -modfile extensions.mod --buildmode=plugin -tags sqlserver -o plugins/sources/Sql.so extensions/sources/sql/sql.go
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
