## 5 minutes quick start

1. Pull a eKuiper Docker image from `https://hub.docker.com/r/lfedge/ekuiper/tags`. It's recommended to use `alpine` image in this tutorial (refer to [eKuiper Docker](https://hub.docker.com/r/lfedge/ekuiper) for the difference of eKuiper Docker image variants). 

2. Set eKuiper source to an MQTT server. This sample uses server locating at `tcp://broker.emqx.io:1883`. `broker.emqx.io` is a public MQTT test server hosted by [EMQ](https://www.emqx.io).

   ```shell
   docker run -p 9081:9081 -d --name kuiper -e MQTT_SOURCE__DEFAULT__SERVER="tcp://broker.emqx.io:1883" lfedge/ekuiper:$tag
   ```

3. Create a stream - the stream is your stream data schema, similar to table definition in database. Let's say the temperature & humidity data are sent to `broker.emqx.io`, and those data will be processed in your **LOCAL RUN** eKuiper docker instance.  Below steps will create a stream named `demo`, and data are sent to `devices/device_001/messages` topic, while `device_001` could be other devices, such as `device_002`, all of those data will be subscribed and handled by `demo` stream.

   ```shell
   -- In host
   # docker exec -it kuiper /bin/sh
   
   -- In docker instance
   # bin/kuiper create stream demo '(temperature float, humidity bigint) WITH (FORMAT="JSON", DATASOURCE="devices/+/messages")'
   Connecting to 127.0.0.1:20498...
   Stream demo is created.
   
   # bin/kuiper query
   Connecting to 127.0.0.1:20498...
   kuiper > select * from demo where temperature > 30;
   Query was submit successfully.
   
   ```

4. Publish sensor data to topic `devices/device_001/messages` of server `tcp://broker.emqx.io:1883` with any MQTT client such as [MQTT X](https://mqttx.app/).

   ```shell
   # mqttx pub -h broker.emqx.io -m '{"temperature": 40, "humidity" : 20}' -t devices/device_001/messages
   ```

5. If everything goes well,  you can see the message is print on docker `bin/kuiper query` window. Please try to publish another message with `temperature` less than 30, and it will be filtered by WHERE condition of the SQL. 

   ```
   kuiper > select * from demo WHERE temperature > 30;
   [{"temperature": 40, "humidity" : 20}]
   ```

   If having any problems, please take a look at `log/stream.log`.

6. To stop the test, just press `ctrl + c` in `bin/kuiper query` command console, or input `exit` and press enter.

You can also refer to [eKuiper dashboard documentation](./operation/manager-ui/overview.md) for better using experience.

Next for exploring more powerful features of eKuiper? Refer to below for how to apply LF Edge eKuiper in edge and integrate with AWS / Azure IoT cloud.

   - [Lightweight edge computing eKuiper and Azure IoT Hub integration solution](https://www.emqx.com/en/blog/lightweight-edge-computing-emqx-kuiper-and-azure-iot-hub-integration-solution) 
   - [Lightweight edge computing eKuiper and AWS IoT Hub integration solution](https://www.emqx.com/en/blog/lightweight-edge-computing-emqx-kuiper-and-aws-iot-hub-integration-solution)