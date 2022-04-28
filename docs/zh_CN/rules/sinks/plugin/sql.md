# Sql 目标（Sink）

此插件将结果写入数据库

## 编译部署插件

此插件必须与至少一个数据库驱动程序一起使用。我们使用构建标签来确定将包含哪个驱动程序。[此处](https://github.com/lf-edge/ekuiper/tree/master/extensions/sqldatabase/driver)列出了所有支持的驱动程序。
该插件默认支持 `sqlserver\postgres\mysql\sqlite3\oracle` 驱动。用户可以自己编译只支持一个驱动的插件，例如如果他只想要 mysql，那么他可以用 build tag mysql 构建。

### 默认构建指令
```shell
# cd $eKuiper_src
# go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/sinks/Sql.so extensions/sinks/sql/sql.go
# cp plugins/sinks/Sql.so $eKuiper_install/plugins/sinks
```

### MySql 构建指令 
```shell
# cd $eKuiper_src
# go build -trimpath -modfile extensions.mod --buildmode=plugin -tags mysql -o plugins/sinks/Sql.so extensions/sinks/sql/sql.go
# cp plugins/sinks/Sql.so $eKuiper_install/plugins/sinks
```


## Properties

| Property name | Optional | Description                                 |
|---------------|----------|---------------------------------------------|
| url           | false    | 目标数据库的url                                   |
| table         | false    | 结果的表名                                       |
| fields        | false    | 要插入的字段。结果映射和数据库都应该有这些字段。如果未指定，将插入结果映射中的所有字段 |
 
## 使用样例

下面是一个获取目标数据并写入 mysql 数据库的示例

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

