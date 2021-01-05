# 流管理

Kuiper 流命令行工具可用于管理流，例如创建、描述、显示和删除流定义。

## 创建流

该命令用于创建流。有关流定义的更多详细信息，请参考[流](../sqls/streams.md)。

```shell
create stream $stream_name $stream_def | create stream -f $stream_def_file
```

- 在命令行中指定流定义。

示例：

```shell
# bin/kuiper create stream my_stream '(id bigint, name string, score float) WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");'
stream my_stream created
```

该命令创建一个名为 `my_stream` 的流。

- 在文件中指定流定义。 如果流很复杂，或者流已经通过明确的格式写在文本文件中，则只需通过 `-f` 选项规定流定义。

示例：

```shell
# bin/kuiper create stream -f /tmp/my_stream.txt
stream my_stream created
```

以下是`my_stream.txt`的内容。

```json
my_stream(id bigint, name string, score float)
    WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");
```

## 显示流

该命令用于显示服务器中定义的所有流。

```shell
show streams
```

示例：

```shell
# bin/kuiper show streams
my_stream
```

## 描述流

该命令用于打印流的详细定义。

```shell
describe stream $stream_name
```

示例：

```shell
# bin/kuiper describe stream my_stream
Fields
--------------------------------------------------------------------------------
id	bigint
name	string
score	float

FORMAT: json
KEY: id
DATASOURCE: topic/temperature
```

## 删除流

该命令用于删除流定义。

```shell
drop stream $stream_name
```

示例：

```shell
# bin/kuiper drop stream my_stream
stream my_stream dropped
```

## 查询流
该命令用于从流中查询数据。
```
query
```

示例：

```shell
# bin/kuiper query
kuiper > 
```

键入 `query` 子命令后，它会提示 `kuiper>`，然后在命令提示符中键入 SQLs （有关如何使用Kuiper SQL的信息，请参阅 [Kuiper SQL 参考](../sqls/overview.md) ），然后按回车。

结果将在控制台中打印。

```shell
kuiper > SELECT * FROM my_stream WHERE id > 10;
[{"...":"..." ....}]
...
```
- 输入 `CTRL + C` 停止查询; 
- 如果没有键入任何 SQL，则可以键入 `quit` 或 `exit` 退出 `kuiper` 提示控制台。
