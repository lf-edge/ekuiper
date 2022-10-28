# 模式管理

eKuiper 模式命令行工具使您可以管理模式，例如创建，显示和删除插件。

## 创建模式

该命令用于创建模式。 模式的定义以 JSON 格式指定。

```shell
create schema $schema_type $schema_name $schema_json
```

模式可以通过两种方式创建。

- 在命令行中指定模式文本。

示例：

```shell
# bin/kuiper create schema protobuf schema1 '{"name": "schema1","content": "message Book {required string title = 1; required int32 price = 2;}"}'
```

该命令创建一个名为 `schema1` 的模式，模式内容由 json 中的 content 指定。

- 在命令行中指定模式位置。

示例：

```shell
# bin/kuiper create schema protobuf schema1 '{"name": "schema1","file": "file:///tmp/aschema.proto"}'
```

该命令创建一个名为 `schema1` 的模式，模式内容由 json 中的 file 指定。文件将被复制到 `data/schemas/protobuf` 下并重命名为 `schema1.proto`。

### 参数

1. schema_type：模式类型，可用值为 `protobuf`。
2. schema_name：模式的唯一名称，模式内容将保存在以此为名的文件中。
3. schema_json：定义模式内容的 json，需要包含 name 以及 file 或 content。


## 显示模式

该命令用于显示服务器中为模式类型定义的所有模式。

```shell
show schemas $schema_type
```

示例：

```shell
# bin/kuiper show schemas protobuf
schema1
schema2
```

## 描述模式

该命令用于打印模式的详细定义。

```shell
describe schema $schema_type $schema_name
```

示例：

```shell
# bin/kuiper describe schema protobuf schema1
{
  "type": "protobuf",
  "name": "schema1",
  "content": "message Book {required string title = 1; required int32 price = 2;}",
  "file": "ekuiper\\etc\\schemas\\protobuf\\schema1.proto"
}

```

## 删除模式

该命令用于删除模式。模式删除后，已经被规则载入的模式仍然可继续使用，但重启之后规则将报错。

```shell
drop schema $schema_type $schema_name
```

示例：

```shell
# bin/kuiper drop schema protobuf schema1
Schema schema1 is dropped.
```