# Zmq Sink

The sink will publish the result into a Zero Mq topic.

## Compile & deploy plugin

```shell
# cd $eKuiper_src
# go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/sinks/Zmq.so extensions/sinks/zmq/zmq.go
# cp plugins/sinks/Zmq.so $eKuiper_install/plugins/sinks
```

Restart the eKuiper server to activate the plugin.

## Properties

| Property name | Optional | Description                   |
|---------------|----------|-------------------------------|
| server        | false    | The url of the Zero Mq server |
| topic         | true     | The topic to publish to       |

## Sample usage

Below is a sample for selecting temperature great than 50 degree, and publish the result into Zero Mq topic "temp".

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

