# InfluxDB 目标（Sink）

该插件将分析结果发送到 InfluxDB V2.X 中。

## 属性

连接相关的属性：

| 属性名称             | 是否可选 | 说明                                                                                                                                                                                                                                                                      |
| -------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| addr                 | 否       | InfluxDB 的地址                                                                                                                                                                                                                                                           |
| token                | 是       | InfluxDB 访问 Token                                                                                                                                                                                                                                                       |
| org                  | 否       | InfluxDB 存储组织                                                                                                                                                                                                                                                         |
| bucket               | 否       | InfluxDB 存储 Bucket                                                                                                                                                                                                                                                      |
| certificationPath    | 是       | 证书路径。可以为绝对路径，也可以为相对路径。如果指定的是相对路径，那么父目录为执行 `kuiperd` 命令的路径。比如，如果你在 `/var/kuiper` 中运行 `bin/kuiperd` ，那么父目录为 `/var/kuiper`; 如果运行从 `/var/kuiper/bin` 中运行`./kuiperd`，那么父目录为 `/var/kuiper/bin`。 |
| privateKeyPath       | 是       | 私钥路径。可以为绝对路径，也可以为相对路径，相对路径的用法与 `certificationPath` 类似。                                                                                                                                                                                   |
| rootCaPath           | 是       | 根证书路径，用以验证服务器证书。可以为绝对路径，也可以为相对路径，相对路径的用法与 `certificationPath` 类似。                                                                                                                                                             |
| renegotiationSupport | true     | Determines how and when the client handles server-initiated renegotiation requests. Support `never`, `once` or `freely` options. Default: `never`.                                                                                                                        |
| insecureSkipVerify   | 是       | 如果 InsecureSkipVerify 设置为 `true`, TLS接受服务器提供的任何证书以及该证书中的任何主机名。 在这种模式下，TLS容易受到中间人攻击。默认值为 `false`。配置项只能用于TLS连接。                                                                                               |

写入相关属性：

| 属性名称        | 是否可选 | 说明                                                                                                                                                                                                                                                                         |
| --------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| measurement     | 否       | InfluxDB 的测量（如表名）                                                                                                                                                                                                                                                    |
| tags            | 是       | 标签键值对，其格式为 {"tag1":"value1"}。其中，值可为数据模板格式，例如 <span v-pre>{"tag1":"{{.temperature}}"}</span>                                                                                                                                                        |
| fields          | 是       | 需要写入的字段列表，格式为 ["field1", "field2"] 。如果该属性未设置，则所有 SQL 中选出的字段都会写入 InfluxDB 。                                                                                                                                                              |
| precision       | 是       | 时间戳精度，若采用自定义时间，需要保证时间精度与此设置相同。 可设置为 `ns`, `us`, `ms`, `s`。默认为 `ms`。                                                                                                                                                                   |
| tsFieldName     | 是       | 时间戳字段名。若有设置，写入时的时间戳以该字段的值为准。例如，假设数据为 {"ts": 1888888888} 且 tsFieldName 属性设置为 ts，则 1888888888 将作为此条数据写入作为的时间戳。此时，需要确保时间戳的值的精度与 precision 的配置相同。 如果该属性未设置，则写入时采用当时的时间戳。 |
| useLineProtocol | 是       | 是否使用[行协议格式](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/)。默认为 false 。若使用行协议写入，设置数据模板属性时，其格式化结果应当按照行协议格式进行格式化。                                                                               |

其他通用的 sink 属性也支持，包括批量设置等，请参阅[公共属性](../overview.md#公共属性)。

## 示例用法

下面是选择温度大于 50 度并写入 influxDB 的示例规则。

```json
{
  "id": "influx",
  "sql": "SELECT * from demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "influx2": {
        "addr": "http://192.168.100.245:8086",
        "token": "test_token",
        "org": "admin",
        "measurement": "test",
        "bucket": "bucketName",
        "tags": "{\"tag1\":\"value1\"}",
        "fields": ["humidity", "temperature", "pressure"]
      }
    }
  ]
}
```
