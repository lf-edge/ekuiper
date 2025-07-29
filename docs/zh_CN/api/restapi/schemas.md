eKuiper REST api 允许您管理模式，例如创建、删除和列出模式。

## 创建模式

该 API 接受 JSON 内容以创建新的模式。 每种模式类型都有一个独立的端点。当前模式类型仅有一种 `protobuf`。模式由名称标识。名称必须唯一。

```shell
POST http://localhost:9081/schemas/protobuf
```

模式内容在请求体上的请求示例：

```json
{
  "name": "schema1",
  "content": "message Book {required string title = 1; required int32 price = 2;}"
}
```

模式内容在文件上的请求示例：

```json
{
  "name": "schema2",
  "file": "file:///tmp/ekuiper/internal/schema/test/test2.proto"
}
```

模式包含静态插件示例：

```json
{
  "name": "schema2",
  "file": "file:///tmp/ekuiper/internal/schema/test/test2.proto",
   "soFile": "file:///tmp/ekuiper/internal/schema/test/so.proto"
}
```

### 参数

1. name：模式的唯一名称。
2. 模式的内容，可选用 file 或 content 参数来指定。模式创建后，模式内容将写入 `data/schemas/$shcema_type/$schema_name` 文件中。
   - file：模式文件的 URL。URL 支持 http 和 https 以及 file 模式。当使用 file 模式时，该文件必须在 eKuiper
     服务器所在的机器上。引用的文件可以是单个模式文件，也可以是 ZIP 压缩文件。
      - 单个模式文件: 文件扩展名必须与相应的模式类型匹配。例如：Protobuf 模式文件必须是 .proto 扩展名。
      - ZIP 压缩文件: ZIP 文件根目录中必须包含一个主模式文件。可选地，ZIP 文件中还可以包含一个与模式名称相同（不带扩展名）的文件夹，用于存放支持文件。ZIP
        归档中的任何其他文件或文件夹都将被忽略。例如：对于名为 test 的模式，test.zip 文件应具有以下结构：

       ```text
         test.zip/
         ├── test.proto  (主文件)
         └── test/          (可选文件夹，包含支持文件)
           ├── helper.proto
           └── config.json
       ```

   - content：模式文件的内容。
3. soFile：静态插件 so。插件创建请看[自定义格式](../../guide/serialization/serialization.md#格式扩展)。

## 显示模式

该 API 用于显示服务器中为模式类型定义的所有模式。

```shell
GET http://localhost:9081/schemas/protobuf
```

响应示例：

```json
["schema1","schema2"]
```

## 描述模式

该 API 用于打印模式的详细定义。

```shell
GET http://localhost:9081/schemas/protobuf/{name}
```

路径参数 `name` 是模式的名称。

响应示例：

```json
{
  "type": "protobuf",
  "name": "schema1",
  "content": "message Book {required string title = 1; required int32 price = 2;}",
  "file": "ekuiper\\etc\\schemas\\protobuf\\schema1.proto"
}
```

## 删除模式

该 API 用于删除模式。

```shell
DELETE http://localhost:9081/schemas/protobuf/{name}
```

## 修改模式

该 API 用于修改模式，其消息体格式与创建时相同。

```shell
PUT http://localhost:9081/schemas/protobuf/{name}

{
  "name": "schema2",
  "file": "http://ahot.com/test2.proto"
}
```
