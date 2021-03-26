# Tables management

The Kuiper table command line tools allows you to manage the tables, such as create, describe, show and drop table definitions.

## create a table

The command is used for creating a table. For more detailed information of table definition, please refer to [tables](../sqls/tables.md).

```shell
create table $table_name $table_def | create table -f $table_def_file
```

- Specify the table definition in command line.

Sample:

```shell
# bin/kuiper create table my_table '(id bigint, name string, score float) WITH ( datasource = "lookup.json", FORMAT = "json", KEY = "id");'
table my_table created
```

The command create a table named ``my_table``. 

- Specify the table definition in file. If the table is complex, or the table is already wrote in text files with well organized formats, you can just specify the table definition through ``-f`` option.

Sample:

```shell
# bin/kuiper create table -f /tmp/my_table.txt
table my_table created
```

Below is the contents of ``my_table.txt``.

```
my_table(id bigint, name string, score float)
    WITH ( datasource = "lookup.json", FORMAT = "json", KEY = "id");
```

## show tables

The command is used for displaying all of tables defined in the server.

```shell
show tables
```

Sample:

```shell
# bin/kuiper show tables
my_table
```

## describe a table

The command is used for print the detailed definition of table.

```shell
describe table $table_name
```

Sample:

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

## drop a table

The command is used for drop the table definition.

```shell
drop table $table_name
```

Sample:

```shell
# bin/kuiper drop table my_table
table my_table dropped
```