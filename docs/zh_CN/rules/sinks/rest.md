# REST动作

该动作用于将输出消息发布到 RESTful API 中。

| 属性名称               | 是否可选 | 说明                                                                                                                                                                                                                  |
|--------------------|------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| method             | 是    | RESTful API 的 HTTP 方法。 这是一个不区分大小写的字符串，其值范围为"get"，"post"，"put"，"patch"，"delete" 和 "head"。 默认值为 "get"，支持动态获取。                                                                                                         |
| url                | 否    | RESTful API 终端地址，例如 `https://www.example.com/api/dummy`，支持动态获取。                                                                                                                                                     |
| bodyType           | 是    | 消息体的类型。 当前，支持以下类型："none", "json", "text", "html", "xml", "javascript"  和 "form"。 对于 "get" 和 "head"，不需要正文，因此默认值为 "none"。 对于其他 http 方法，默认值为 "json"。对于 "html"，"xml" 和 "javascript"，必须仔细设置 dataTemplate 以确保格式正确。支持动态获取。 |
| timeout            | 是    | HTTP 请求超时的时间（毫秒），默认为5000毫秒                                                                                                                                                                                          |
| headers            | 是    | 要为 HTTP 请求设置的其它 HTTP 头。支持动态获取。                                                                                                                                                                                      |
| debugResp          | 是    | 控制是否将响应信息打印到控制台中。 如果将其设置为 `true`，则打印响应。 如果设置为`false`，则跳过打印日志。 默认值为 `false`。                                                                                                                                         |
| certificationPath  | 是    | 证书路径。可以为绝对路径，也可以为相对路径。如果指定的是相对路径，那么父目录为执行 `kuiperd` 命令的路径。比如，如果你在 `/var/kuiper` 中运行 `bin/kuiperd` ，那么父目录为 `/var/kuiper`; 如果运行从 `/var/kuiper/bin` 中运行`./kuiperd`，那么父目录为 `/var/kuiper/bin`。                           |
| privateKeyPath     | 是    | 私钥路径。可以为绝对路径，也可以为相对路径，相对路径的用法与 `certificationPath` 类似。                                                                                                                                                              |
| rootCaPath         | 是    | 根证书路径，用以验证服务器证书。可以为绝对路径，也可以为相对路径，相对路径的用法与 `certificationPath` 类似。                                                                                                                                                   |
| insecureSkipVerify | 是    | 控制是否跳过证书认证。如果被设置为 `true`，那么跳过证书认证；否则进行证书验证。缺省为 `true`。                                                                                                                                                              |

::: v-pre
REST 服务通常需要特定的数据格式。 这可以由公共目标属性 `dataTemplate` 强制使用。 请参考[数据模板](../overview.md#数据模板)。 以下是用于连接到 Edgex Foundry core 命令的示例配置。dataTemplate`{{.key}}` 表示将打印出键值，即 result [key]。 因此，这里的模板是在结果中仅选择字段 `key` ，并将字段名称更改为 `newKey`。 `sendSingle` 是另一个常见属性。 设置为 true 表示如果结果是数组，则每个元素将单独发送。
:::

```json
    {
      "rest": {
        "url": "http://127.0.0.1:59882/api/v1/device/cc622d99-f835-4e94-b5cb-b1eff8699dc4/command/51fce08a-ae19-4bce-b431-b9f363bba705",
        "method": "post",
        "dataTemplate": "\"newKey\":\"{{.key}}\"",
        "sendSingle": true
      }
    }
```

Visualization mode
以可视化图形交互创建rules的SQL和Actions

Text mode
以json格式创建rules的SQL和Actions

创建写taosdb rest示例：

```json
{"id": "rest1",
  "sql": "SELECT tele[0]-\u003eTag00001 AS temperature, tele[0]-\u003eTag00002 AS humidity FROM neuron", 
  "actions": [
    {
      "rest": {
        "bodyType": "text",
        "dataTemplate": "insert into mqtt.kuiper values (now, {{.temperature}}, {{.humidity}})", 
        "debugResp": true,
        "headers": {"Authorization": "Basic cm9vdDp0YW9zZGF0YQ=="},
        "method": "POST",
        "sendSingle": true,
        "url": "http://xxx.xxx.xxx.xxx:6041/rest/sql"
      }
    }
  ]
}
```

## 设置动态输出参数

很多情况下，我们需要根据结果数据，决定写入的目的地址和参数。在 REST sink 里，`method`, `url`, `bodyType` 和 `headers` 支持动态参数。动态参数可通过数据模板语法配置。接下来，让我们使用动态参数改写上例。假设我们收到了数据中包含了 http 方法和 url 后缀等元数据。我们可以通过改写 SQL 语句，在输出结果中得到这两个值。规则输出的单条数据类似：

```json
{
  "method":"post",
  "url":"http://xxx.xxx.xxx.xxx:6041/rest/sql",
  "temperature": 20,
  "humidity": 80
}
```

在规则 action 中，可以通过数据模板语法取得结果数据作为属性变量。如下例子中，`method` 和 `url` 为动态变量。

```json
{"id": "rest2",
  "sql": "SELECT tele[0]->Tag00001 AS temperature, tele[0]->Tag00002 AS humidity, method, concat(\"http://xxx.xxx.xxx.xxx:6041/rest/sql\", urlPostfix) as url FROM neuron", 
  "actions": [
    {
      "rest": {
        "bodyType": "text",
        "dataTemplate": "insert into mqtt.kuiper values (now, {{.temperature}}, {{.humidity}})", 
        "debugResp": true,
        "headers": {"Authorization": "Basic cm9vdDp0YW9zZGF0YQ=="},
        "method": "{{.method}}",
        "sendSingle": true,
        "url": "{{.url}}"
      }
    }
  ]
}
```