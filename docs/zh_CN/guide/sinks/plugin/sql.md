# SQL 目标（Sink）

此插件将结果写入 SQL 数据库

## 编译部署插件

此插件必须与至少一个数据库驱动程序一起使用。我们使用构建标签来确定将包含哪个驱动程序。[eKuiper - SQL 数据库插件 GitHub 页面](https://github.com/lf-edge/ekuiper/tree/master/extensions/sqldatabase/driver)列出了所有支持的驱动程序。
该插件默认支持 `sqlserver\postgres\mysql\sqlite3\oracle` 驱动。用户可以自己编译只支持一个驱动的插件，例如如果他只想要 MySQL，那么他可以用 build tag mysql 构建。

当使用 `sqlserver` 作为目标 source 时，需要确认该 `sqlserver` 暴露了 1434 端口。

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

你可以通过 api 的方式提前检查对应 sink 端点的连通性: [连通性检查](../../../api/restapi/connection.md#连通性检查)

### 动态字段名

未配置 `fields` 时，SQL sink 使用结果映射的 key 作为列名；批量写入时以第一行的 key 为准。每个生成的列名必须符合 `[A-Za-z_][A-Za-z0-9_]*`：第一个字符必须是 ASCII 字母或下划线，后续字符只能是 ASCII 字母、数字或下划线。如果 `rowkindField` 指定的 key 会作为列写入，该 key 也受此限制。

如果生成的列名不符合该格式，本次写入会在执行 SQL 前报错。SQL sink 不会静默丢弃该 key，也不会自动为其添加引号。

显式配置的 `table`、`fields` 和 `keyField` 会原样写入生成的 SQL，以继续支持不同数据库的标识符语法。每个 `fields` 配置项同时用于从结果映射中查找值，因此映射 key 必须与配置项完全一致，并且配置项必须使用目标数据库接受的语法。

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
