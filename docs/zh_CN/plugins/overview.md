Kuiper 实现了几个插件。

## 源（Sources）

| 名称                  | 描述                                                  |
| --------------------- | ------------------------------------------------------------ |
| [zmq](sources/zmq.md)| 该插件监听 Zero Mq 消息并发送到 Kuiper 流中 |
| [random](sources/random.md) | 该插件按照指定模式生成消息   |



## 动作（Sinks/Actions）



| 名称                  | 描述                                                  |
| --------------------- | ------------------------------------------------------------ |
| [file](sinks/file.md) | 该插件将分析结果保存到某个指定到文件系统中 |
| [zmq](sinks/zmq.md)   | 该插件将分析结果发送到 Zero Mq 的主题中  |
| [influxdb](sinks/influxdb.md)   | 该插件将分析结果发送到 InfluxDB 中  |




## 函数（Functions）

...

