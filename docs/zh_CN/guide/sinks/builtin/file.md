# 文件目标（Sink）

该 sink 将分析结果保存到指定文件中。[文件源](../../sources/builtin/file.md)是反向的连接器可以读取文件 sink 的输出。

## 属性

| 属性名称      | 是否可选 | 说明                                                            |
|-----------|------|---------------------------------------------------------------|
| path      | 否    | 保存结果的文件路径，例如  `/tmp/result.txt`                               |
| interval  | 是    | 写入分析结果的时间间隔（毫秒）。 默认值为1000，这表示每隔一秒钟写入一次分析结果。                   |
| fileType  | 是    | 文件类型，支持 json， csv 或者 lines，其中默认值为 lines。更多信息请参考[文件类型](#文件类型)。 |
| hasHeader | 是    | 指定是否生成文件头。当前仅在文件类型为 csv 时生效。                                  |
| delimiter | 是    | 指定分隔符，通常用于文件头的生成。当前仅当文件类型为 csv 时生效。                           |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。其中，`format` 属性用于定义文件中数据的格式。某些文件类型只能与特定格式一起使用，详情请参阅[文件类型](#文件类型)。

### 文件类型

文件 sink 可以将数据写入不同类型的文件，例如：

- lines：这是默认类型。它写入由流定义中的格式参数解码的行分隔文件。例如，要写入行分隔的 JSON 字符串，请将文件类型设置为 lines，格式设置为 json。
- json：此类型写入标准 JSON 数组格式文件。有关示例，请参见[此处](https://github.com/lf-edge/ekuiper/tree/master/internal/topo/source/test/test.json)。要使用此文件类型，请将格式设置为 json。
- csv：此类型写入逗号分隔的 csv 文件。您也可以使用自定义分隔符。要使用此文件类型，请将格式设置为 delimited。"

## 使用示例

下面是一个选择温度大于50度的示例，每5秒将结果保存到文件 `/tmp/result.txt`  中。

```json
{
  "sql": "SELECT * from demo where temperature>50",
  "actions": [
    {
      "file": {
        "path": "/tmp/result.txt",
        "interval": 5000,
        "fileType": "lines",
        "format": "json"
      }
    }
  ]
}
```

