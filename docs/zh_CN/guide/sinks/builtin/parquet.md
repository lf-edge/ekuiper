# Parquet 动作

该动作用于将分析结果保存到 [Apache Parquet](https://parquet.apache.org/) 文件中。文件的列及其类型在规则中声明。

::: tip
只有在使用 `parquet` 或 `full` 构建标签编译 eKuiper 时才能使用此动作，例如 `go build -tags parquet ./cmd/kuiperd`。
:::

## 属性

唯一新增的属性是 `columns`，且为必填项。`path` 属性和[滚动属性](./file.md#rolling-策略)的用法与[文件动作](./file.md)相同，但有以下区别：

- `path` 不支持模板。
- 不支持 `rollingSize`、`compression`、`encryption` 和 `rollingHook`。使用这些属性的规则将无法创建。
- 如果未设置 `rollingNamePattern` 或将其设置为 `none`，每次滚动和每次规则重启都会替换上一个文件。如需保留所有文件，请使用 `prefix` 或 `suffix`。
- 请不要设置 `format` 属性。它仅用于定义如何解析 `dataTemplate` 的输出。

数据行缓存在内存中，可能要到文件关闭时才会写入磁盘。如果进程崩溃，当前打开文件中的数据将丢失。

### 列

`columns` 属性声明每一列及其类型：

```json
{
  "columns": {
    "id": "int64",
    "temperature": "double",
    "location": "string?",
    "active": "boolean"
  }
}
```

支持的类型为 `boolean`、`int64`、`double` 和 `string`，不区分大小写。列默认是必需的。在类型后添加 `?` 可使该列允许为空。

无论配置中的顺序如何，列始终按字母顺序存储。

### 类型检查

值不会被转换。每个值必须已经是其所在列的确切类型，否则该行将被拒绝。被拒绝的行会报告为规则错误且不会被写入，规则将继续运行。如果批次中有一行被拒绝，则整个批次都不会被写入。

| 数据行               | 结果       |
|-------------------|----------|
| 值的类型不同            | 该行被拒绝    |
| 字段未在 `columns` 中声明 | 该行被拒绝    |
| 必需列的值为 `null`     | 该行被拒绝    |
| 可为空列的值为 `null`    | 写入 null  |
| 缺少可为空列            | 写入 null  |
| 缺少必需列             | 写入该类型的零值 |

零值分别为：`int64` 和 `double` 为 `0`，`string` 为空字符串，`boolean` 为 `false`。零值无法与真实值区分，因此如果某个值可能缺失，请将该列声明为可为空。

规则输出的每个字段都必须声明。例如，只有当 `columns` 列出所有字段时，`SELECT *` 才能正常工作。如果只想保存部分字段，请在 SQL 语句中选择这些字段，或使用 `fields` 通用属性。

::: warning
使用此动作时，请声明流的 schema。如果没有 schema，JSON 数字将被解码为 `double`，此时 `int64` 列会拒绝每一行，即使值为 `21`
也是如此。如果改为设置 `useInt64ForWholeNumber`，则 `double` 列会拒绝 `21` 这样的整数。`dataTemplate` 的输出也以同样的方式解码，因此
`dataTemplate` 无法填充 `int64` 列。
:::

## 示例

以下示例保存温度大于 50 度的数据。流声明了 schema，因此值的类型与列的类型一致：

```sql
CREATE STREAM demo (id BIGINT, temperature FLOAT, location STRING) WITH (DATASOURCE="demo", FORMAT="json")
```

每小时或每 10000 行创建一个新文件，文件名类似于 `result-1699888888.parquet`。

```json
{
  "sql": "SELECT id, temperature, location FROM demo WHERE temperature > 50",
  "actions": [
    {
      "parquet": {
        "path": "result.parquet",
        "columns": {
          "id": "int64",
          "temperature": "double",
          "location": "string?"
        },
        "rollingInterval": 3600000,
        "checkInterval": 600000,
        "rollingCount": 10000,
        "rollingNamePattern": "suffix"
      }
    }
  ]
}
```
