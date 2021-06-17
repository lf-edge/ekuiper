# Zmq 目标（Sink）

目标（Sink）会将结果发布到 Zero Mq 主题中。

## 编译和部署插件

```shell
# cd $ekuiper_src
# go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/sinks/Zmq.so extensions/sinks/zmq/zmq.go
# cp plugins/sinks/Zmq.so $ekuiper_install/plugins/sinks
```

重新启动 eKuiper 服务器以激活插件。

## 属性

| 属性名称 | Optional | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| server          | false    | Zero Mq 服务器的 URL |
| topic      | true     | The topic to publish to |

## 使用示例

下面是一个选择温度大于50度的示例，并将结果发布到 Zero Mq 主题 "temp"。

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

