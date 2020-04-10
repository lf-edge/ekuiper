## 5 分钟快速入门

1. 从 ``https://hub.docker.com/r/emqx/kuiper/tags`` 拉一个 Kuiper 的 docker 镜像。

2. 设置 Kuiper 源为一个 MQTT 服务器。本例使用位于 ``tcp://broker.emqx.io:1883`` 的 MQTT 服务器， ``broker.emqx.io`` 是一个由 [EMQ](https://www.emqx.io) 提供的公有MQTT 服务器。

   ```shell
   docker run -d --name kuiper -e MQTT_BROKER_ADDRESS=tcp://broker.emqx.io:1883 emqx/kuiper:$tag
   ```

3. 创建流（stream）- 流式数据的结构定义，类似于数据库中的表格类型定义。比如说要发送温度与湿度的数据到 ``broker.emqx.io``，这些数据将会被在**本地运行的** Kuiper docker 实例中处理。以下的步骤将创建一个名字为 ``demo``的流，并且数据将会被发送至 ``devices/device_001/messages`` 主题，这里的 ``device_001`` 可以是别的设备，比如 ``device_002``，所有的这些数据会被 ``demo`` 流订阅并处理。

   ```shell
   -- In host
   # docker exec -it kuiper /bin/sh
   
   -- In docker instance
   # bin/cli create stream demo '(temperature float, humidity bigint) WITH (FORMAT="JSON", DATASOURCE="devices/+/messages")'
   Connecting to 127.0.0.1:20498...
   Stream demo is created.
   
   # bin/cli query
   Connecting to 127.0.0.1:20498...
   kuiper > select * from demo where temperature > 30;
   Query was submit successfully.
   
   ```

4. 您可以使用任何[ MQTT 客户端工具](https://www.emqx.io/cn/blog/mqtt-client-tools)来发布传感器数据到服务器 ``tcp://broker.emqx.io:1883``的主题 ``devices/device_001/messages`` 。以下例子使用 ``mosquitto_pub``。

   ```shell
   # mosquitto_pub -h broker.emqx.io -m '{"temperature": 40, "humidity" : 20}' -t devices/device_001/messages
   ```

5. 如果一切顺利的话，您可以看到消息打印在容器的 ``bin/cli query`` 窗口里，请试着发布另外一条``温度``小于30的数据，该数据将会被 SQL 规则过滤掉。

   ```shell
   kuiper > select * from demo WHERE temperature > 30;
   [{"temperature": 40, "humidity" : 20}]
   ```

   如有任何问题，请查看日志文件 ``log/stream.log``。

6. 如果想停止测试，在``bin/cli query``命令行窗口中敲 ``ctrl + c `` ，或者输入 ``exit`` 后回车

7. 想了解更多 EMQ X Kuiper 的功能？请参考以下关于在边缘端使用 EMQ X Kuiper 与 AWS / Azure IoT 云集成的案例。

   - [轻量级边缘计算 EMQ X Kuiper 与 AWS IoT 集成方案](https://www.jianshu.com/p/7c0218fd1ee2)
   - [轻量级边缘计算 EMQ X Kuiper 与 Azure IoT Hub 集成方案](https://www.jianshu.com/p/49b06751355f) 