# Zmq 目标（Sink）

目标（Sink）会将结果发布到Zero Mq 主题中。

## 编译和部署插件

```shell
# cd $kuiper_src
# go build --buildmode=plugin -o plugins/sinks/Zmq.so plugins/sinks/zmq.go
# cp plugins/sinks/Zmq.so $kuiper_install/plugins/sinks
```

重新启动Kuiper服务器以激活插件。

## 属性

|属性名称 | 是否可选 | 描述                                                 |
| ------------- | -------- | ------------------------------------------------------------ |
| server          | 否    | Zero Mq 服务器的URL |
| topic      | 是    | 发布的目标主题 |

## 使用示例

下面是一个选择温度大于50度的示例，并将结果发布到Zero Mq 主题 "temp"。

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

