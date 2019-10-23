# MQTT action

The action is used for publish output message into a MQTT server. 

| Property name | Optional | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| server        | false    | The broker address of the mqtt server, such as ``tcp://127.0.0.1:1883`` |
| topic         | false    | The mqtt topic, such as ``analysis/result``                  |
| clientId      | true     | The client id for mqtt connection. If not specified, an uuid will be used |

