eKuiper REST api allows you to manage Config Keys, e.g. list, delete, register.

## List all configKey

This API is used to get all Config Keys under a specific source name

```shell
GET http://localhost:9081/metadata/sources/yaml/{name}
```
### Parameter
 
 name：Source name, supports built-in sources and extended sources. The built-in sources include mqtt, redis, neuron, memory, httppull, httppush, file, edgex,
 Extended sources include random, sql, video, zmq and user-defined sources


### Example

Example request to get all Config Keys from an MQTT source:

```shell
 curl http://localhost:9081/metadata/sources/yaml/mqtt
```

```json
{
	"amd_broker": {
		"insecureSkipVerify": false,
		"protocolVersion": "3.1.1",
		"qos": 1,
		"server": "tcp://122.9.166.75:1883"
	},
	"default": {
		"qos": 2,
		"server": "tcp://emqx:1883"
	},
	"demo_conf": {
		"qos": 0,
		"server": "tcp://10.211.55.6:1883"
	}
}
```

## Delete a configKey

This API is used to delete a Config Key configuration under a specific source name

```shell
DELETE http://localhost:9081/metadata/sources/{name}/confKeys/{confKey}
```
### Parameter

1. name：Source name, supports built-in sources and extended sources. The built-in sources include mqtt, redis, neuron, memory, httppull, httppush, file, edgex,
   Extended sources include random, sql, video, zmq and user-defined sources
2. confKey: Config Key Name。Taking the above as an example, the Config Keys are amd_broker, default, demo_conf in sequence.


### Example

Delete the Config Key named demo_conf under the MQTT source

```shell
 curl -X DELETE http://localhost:9081/metadata/sources/mqtt/confKeys/demo_conf
```

## Register a configKey

This API is used to register a Config Key under a specific source name

```shell
PUT http://localhost:9081/metadata/sources/{name}/confKeys/{confKey}
```
### Parameter

1. name：Source name, supports built-in sources and extended sources. The built-in sources include mqtt, redis, neuron, memory, httppull, httppush, file, edgex,
   Extended sources include random, sql, video, zmq and user-defined sources
2. confKey: Config Key name to register


### Example

Register the Config Key named demo_conf under the MQTT source

```shell
 curl -X PUT http://localhost:9081/metadata/sources/mqtt/confKeys/demo_conf
 {
   "demo_conf": {
		"qos": 0,
		"server": "tcp://10.211.55.6:1883"
	}
 }
```
