# REST动作

该动作用于将输出消息发布到 RESTful API 中。

| 属性名称   | 是否可选 | 说明                                                  |
| ----------------- | -------- | ------------------------------------------------------------ |
| method            | 是    | RESTful API 的 HTTP 方法。 这是一个不区分大小写的字符串，其值范围为"get"，"post"，"put"，"patch"，"delete" 和 "head"。 默认值为 "get"。 |
| url             | 否    | RESTful API 终端地址，例如 `https://www.example.com/api/dummy` |
| bodyType          | 是    | 消息体的类型。 当前，支持以下类型："none", "json", "text", "html", "xml", "javascript"  和 "form"。 对于 "get" 和 "head"，不需要正文，因此默认值为 "none"。 对于其他 http 方法，默认值为 "json"。对于 "html"，"xml" 和 "javascript"，必须仔细设置 dataTemplate 以确保格式正确。 |
| timeout   | 是    | HTTP 请求超时的时间（毫秒），默认为5000毫秒 |
| headers            | 是    | 要为 HTTP 请求设置的其它 HTTP 头。 |
| debugResp | 是 | 控制是否将响应信息打印到控制台中。 如果将其设置为 `true`，则打印响应。 如果设置为`false`，则跳过打印日志。 默认值为 `false`。 |
| insecureSkipVerify | 是 | 控制是否跳过证书认证。如果被设置为 `true`，那么跳过证书认证；否则进行证书验证。缺省为 `true`。 |

::: v-pre
REST 服务通常需要特定的数据格式。 这可以由公共目标属性 `dataTemplate` 强制使用。 请参考[数据模板](../overview.md#数据模板)。 以下是用于连接到 Edgex Foundry core 命令的示例配置。dataTemplate`{{.key}}` 表示将打印出键值，即 result [key]。 因此，这里的模板是在结果中仅选择字段 `key` ，并将字段名称更改为 `newKey`。 `sendSingle` 是另一个常见属性。 设置为 true 表示如果结果是数组，则每个元素将单独发送。
:::

```json
    {
      "rest": {
        "url": "http://127.0.0.1:48082/api/v1/device/cc622d99-f835-4e94-b5cb-b1eff8699dc4/command/51fce08a-ae19-4bce-b431-b9f363bba705",
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
