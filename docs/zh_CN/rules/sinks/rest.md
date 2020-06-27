# REST动作

该动作用于将输出消息发布到RESTful API中。

| Property name     | Optional | Description                                                  |
| ----------------- | -------- | ------------------------------------------------------------ |
| method            | true    | RESTful API的http方法。 这是一个不区分大小写的字符串，其值范围为“ get”，“ post”，“ put”，“ patch”，“ delete”和“ head”。 默认值为“ get”。 |
| url             | false    | RESTful API终端地址，例如 ``https://www.example.com/api/dummy`` |
| bodyType          | 是    | 消息体的类型。 当前，支持以下类型："none", "json", "text", "html", "xml", "javascript" 和 "form"。 对于“ get”和“ head”，不需要正文，因此默认值为“ none”。 对于其他http方法，默认值为“ json”。对于“ html”，“ xml”和“ javascript”，必须仔细设置dateTemplate以确保格式正确。 |
| timeout   | 是    | http请求超时的时间（毫秒），默认为5000毫秒 |
| headers            | 是    | 要为http请求设置的其他标头。 |
| debugResp | 是 | 控制是否将信息打印到控制台中。 如果将其设置为`true`，则打印响应。 如果设置为`false`，则跳过打印日志。 默认值为`false`。 |

REST服务通常需要特定的数据格式。 这可以由公共目标属性`dataTemplate`强制使用。 请参考[数据模板](../overview.md#data-template)。 以下是用于连接到Edgex Foundry core命令的示例配置。 dataTemplate``{{.key}}``表示将打印出键值，即result [key]。 因此，这里的模板是在结果中仅选择字段``key`` ，并将字段名称更改为``newKey``。 `sendSingle`是另一个常见属性。 设置为true表示如果结果是数组，则每个元素将单独发送。

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