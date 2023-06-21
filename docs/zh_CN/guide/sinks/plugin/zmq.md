# ZeroMQ 目标（Sink）

目标（Sink）会将结果发布到 ZeroMQ 主题中。

## 编译和部署插件

```shell
# cd $ekuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sinks/Zmq.so extensions/sinks/zmq/zmq.go
# cp plugins/sinks/Zmq.so $ekuiper_install/plugins/sinks
```

重新启动 eKuiper 服务器以激活插件。

## 属性

| 属性名称 | Optional | Description         |
| -------- | -------- | ------------------- |
| server   | false    | ZeroMQ 服务器的 URL |
| topic    | true     | 待发送的主题        |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

## 使用示例

下面是一个选择温度大于 50 度的示例，并将结果发布到 ZeroMQ 主题 "temp"。

```json
{
  "sql": "SELECT * from demo where temperature>50",
  "actions": [
    {
      "zmq": {
        "server": "tcp://127.0.0.1:5563",
        "topic": "temp"
      }
    }
  ]
}
```

