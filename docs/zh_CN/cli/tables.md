# 表管理
eKuiper 表命令行工具允许您管理表，例如创建、描述、显示和删除。

## 创建表
该命令用于创建表。更详细的表定义信息请参考[tables](../sqls/tables.md)。
```shell
create table $table_name $table_def | create table -f $table_def_file
```

- 在命令行中定义表信息.
  以下例子通过命令行创建了一个名为 ``my_table``的表。
```shell
# bin/kuiper create table my_table '(id bigint, name string, score float) WITH ( datasource = "lookup.json", FORMAT = "json", KEY = "id");'
table my_table created
```


- 在文件中指定表定义。如果表格很复杂，或者表格已经写在具有良好组织格式的文本文件中，您可以通过“-f”选项指定表格定义。
  例如:
```shell
# bin/kuiper create table -f /tmp/my_table.txt
table my_table created
```

  以下是 ``my_table.txt``的内容.
```
my_table(id bigint, name string, score float)
    WITH ( datasource = "lookup.json", FORMAT = "json", KEY = "id");
```

## 查看所有的表 
该命令用于显示 eKuiper 中定义的所有表。
```shell
show tables
```

例如:
```shell
# bin/kuiper show tables
my_table
```

## 查看表的详细信息

该命令用于打印表的详细定义。
```shell
describe table $table_name
```

例如:
```shell
# bin/kuiper describe table my_table
Fields
--------------------------------------------------------------------------------
id	bigint
name	string
score	float

FORMAT: json
KEY: id
DATASOURCE: lookup.json
```

*注意*： 目前 eKuiper 不支持查看表的内容，用户可直接将表与流 join 查看结果。

## 删除表

该命令用于删除表
```shell
drop table $table_name
```

例如:
```shell
# bin/kuiper drop table my_table
table my_table dropped
```