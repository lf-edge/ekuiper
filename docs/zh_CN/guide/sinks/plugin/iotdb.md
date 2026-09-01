# IoTDB 目标（Sink）

该插件通过原生 Thrift RPC 客户端将分析结果写入 Apache IoTDB，同时支持树模型和表模型。

## 编译插件

在 eKuiper 项目主目录运行如下命令：

```shell
go build -trimpath --buildmode=plugin -o plugins/sinks/Iotdb.so extensions/sinks/iotdb/*.go
```

## 属性

连接属性：

| 属性名称   | 是否可选 | 默认值          | 说明                                              |
|------------|----------|-----------------|---------------------------------------------------|
| addr       | 否       | 127.0.0.1:6667  | IoTDB 服务端地址，格式为 `host:port`。            |
| username   | 是       | root            | 用户名。                                          |
| password   | 是       | root            | 密码。                                            |
| nodeUrls   | 是       | []              | 集群节点列表，设置后覆盖 `addr`。                 |
| timeout    | 是       | 5000            | 连接超时时间，单位毫秒。                          |
| poolSize   | 是       | 3               | 连接池大小。                                      |

模型选择：

| 属性名称 | 是否可选 | 默认值 | 说明                                      |
|----------|----------|--------|-------------------------------------------|
| model    | 否       | tree   | 数据模型：`tree` 树模型，`table` 表模型。 |

树模型属性（当 `model=tree` 时使用）：

| 属性名称   | 是否可选 | 默认值 | 说明                                                 |
|------------|----------|--------|------------------------------------------------------|
| device     | 是       | ""     | 设备路径，例如 `root.sg1.dev1`。树模型必填。         |
| isAligned  | 是       | false  | 是否使用对齐时间序列。                               |

表模型属性（当 `model=table` 时使用）：

| 属性名称         | 是否可选 | 默认值 | 说明                                                                                   |
|------------------|----------|--------|----------------------------------------------------------------------------------------|
| database         | 是       | ""     | 数据库名（不带 `root.` 前缀，自动去除）。表模型必填。                                  |
| table            | 是       | ""     | 目标表名。表模型必填。                                                                 |
| columnCategories | 是       | []     | 列类别列表，与 `measurements` 一一对应：`TAG`、`FIELD`、`ATTRIBUTE`。表模型必填。      |

数据映射属性：

| 属性名称     | 是否可选 | 默认值 | 说明                                                                                                           |
|--------------|----------|--------|----------------------------------------------------------------------------------------------------------------|
| measurements | 否       | []     | 测点/列名列表。                                                                                                |
| dataTypes    | 否       | []     | 与 `measurements` 一一对应的数据类型列表：`INT32`、`INT64`、`FLOAT`、`DOUBLE`、`BOOLEAN`、`TEXT`、`STRING`、`TIMESTAMP`。 |
| tsFieldName  | 是       | ""     | 时间戳字段名（毫秒级）。若为空则使用当前时间；配置后每一行都必须包含该字段。                                      |
| batchSize    | 是       | 10     | 单次 tablet 写入行数。                                                                                         |

其他通用的 sink 属性也支持，包括批量设置等，请参阅[公共属性](../overview.md#公共属性)。

## 示例用法

### 树模型示例

下面是选择温度大于 50 度并使用树模型写入 IoTDB 的示例规则。

```json
{
  "id": "iotdb_tree",
  "sql": "SELECT * from demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "iotdb": {
        "addr": "127.0.0.1:6667",
        "username": "root",
        "password": "root",
        "model": "tree",
        "device": "root.sg1.d1",
        "measurements": ["temperature", "humidity"],
        "dataTypes": ["FLOAT", "FLOAT"],
        "tsFieldName": "ts",
        "batchSize": 10
      }
    }
  ]
}
```

### 表模型示例

下面是选择温度大于 50 度并使用表模型写入 IoTDB 的示例规则。

```json
{
  "id": "iotdb_table",
  "sql": "SELECT * from demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "iotdb": {
        "addr": "127.0.0.1:6667",
        "username": "root",
        "password": "root",
        "model": "table",
        "database": "iot_data",
        "table": "sensor_data",
        "measurements": ["device_id", "temperature", "humidity"],
        "dataTypes": ["STRING", "FLOAT", "FLOAT"],
        "columnCategories": ["TAG", "FIELD", "FIELD"],
        "tsFieldName": "ts",
        "batchSize": 10
      }
    }
  ]
}
```

## 数据类型

`dataTypes` 属性支持以下 IoTDB 数据类型：

| 数据类型   | 说明              |
|------------|-------------------|
| INT32      | 32 位有符号整数   |
| INT64      | 64 位有符号整数   |
| FLOAT      | 单精度浮点数      |
| DOUBLE     | 双精度浮点数      |
| BOOLEAN    | 布尔值            |
| TEXT       | 文本（IoTDB 传统类型） |
| STRING     | 字符串            |
| TIMESTAMP  | 时间戳            |

## 注意事项

- IoTDB 默认使用 Thrift RPC 协议，端口为 6667。
- 表模型下，如果数据库不存在会自动创建。
- 表模型的 `database` 字段会自动去除 `root.` 前缀。
- 数据中缺失的字段将作为 null 值写入 IoTDB。
