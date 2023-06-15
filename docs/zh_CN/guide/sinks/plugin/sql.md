# SQL 目标（Sink）

此插件将结果写入 SQL 数据库

## 编译部署插件

此插件必须与至少一个数据库驱动程序一起使用。我们使用构建标签来确定将包含哪个驱动程序。[eKuiper - SQL 数据库插件 GitHub 页面](https://github.com/lf-edge/ekuiper/tree/master/extensions/sqldatabase/driver)列出了所有支持的驱动程序。
该插件默认支持 `sqlserver\postgres\mysql\sqlite3\oracle` 驱动。用户可以自己编译只支持一个驱动的插件，例如如果他只想要 MySQL，那么他可以用 build tag mysql 构建。

### 默认构建指令

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sinks/Sql.so extensions/sinks/sql/sql.go
# cp plugins/sinks/Sql.so $eKuiper_install/plugins/sinks
```

### MySQL 构建指令

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -tags mysql -o plugins/sinks/Sql.so extensions/sinks/sql/sql.go
# cp plugins/sinks/Sql.so $eKuiper_install/plugins/sinks
```


## 属性

| 属性名称  | 是否可选 | 说明                                                  |
| -------------- | -------- | ------------------------------------------------------------ |
| url            | 否    | 目标数据库的 url                                             |
| table          | 否    | 结果的表名                                                   |
| fields         | 是     | 要插入的字段。结果映射和数据库都应该有这些字段。如果未指定，将插入结果映射中的所有字段 |
| tableDataField | 是     | 将 tableDataField 的嵌套值写入数据库。                       |
| rowkindField   | 是     | 指定哪个字段表示操作，例如插入或更新。如果不指定，默认所有的数据都是插入操作 |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

## 使用样例

下面是一个获取目标数据并写入 MySQL 数据库的示例

```json
{
  "id": "rule",
  "sql": "SELECT stuno as id, stuName as name, format_time(entry_data,\"YYYY-MM-dd HH:mm:ss\") as registerTime FROM SqlServerStream",
  "actions": [
    {
      "log": {
      },
      "sql": {
        "url": "mysql://user:test@140.210.204.147/user?parseTime=true",
        "table": "test",
        "fields": ["id","name","registerTime"]
      }
    }
  ]
}
```

根据 tableDataField 配置将结果写入数据库:

以下配置将 telemetry 字段的对应值写入数据库

```json
{
  "telemetry": [{
    "temperature": 32.32,
    "humidity": 80.8,
    "ts": 1388082430
  },{
    "temperature": 34.32,
    "humidity": 81.8,
    "ts": 1388082440
  }]
}
```

```json lines
{
  "id": "rule",
  "sql": "SELECT telemetry FROM dataStream",
  "actions": [
    {
      "log": {
      },
      "sql": {
        "url": "mysql://user:test@140.210.204.147/user?parseTime=true",
        "table": "test",
        "fields": ["temperature","humidity"],
        "tableDataField":  "telemetry",
      }
    }
  ]
}
```

### 更新示例

通过指定 `rowkindField` 和 `keyField` 属性，sink 可以生成针对主键的插入、更新或删除语句。

```json
{
  "id": "ruleUpdateAlert",
  "sql":"SELECT * FROM alertStream",
  "actions":[
    {
      "sql": {
        "url": "sqlite://test.db",
        "keyField": "id",
        "rowkindField": "action",
        "table": "alertTable",
        "sendSingle": true
      }
    }
  ]
}
```
