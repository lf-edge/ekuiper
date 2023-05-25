eKuiper REST api 允许您管理 Config Key，例如列出、删除、注册。

## 列出所有 configKey 

该 API 用于获取特定源名下所有 Config Key 

```shell
GET http://localhost:9081/metadata/sources/yaml/{name}
```
### 参数
 
 name：源名称，支持内置源和扩展源。其中内置源包括 mqtt、redis、neuron、memory、httppull、httppush、file、edgex,
 扩展源包括 random、sql、video、zmq 以及用户自定义源


### 示例

获取 MQTT 源所有 Config Key 请求示例：

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

## 删除某个 configKey 

该 API 用于删除特定源名下某个 Config Key 

```shell
DELETE http://localhost:9081/metadata/sources/{name}/confKeys/{confKey}
```
### 参数

1. name：源名称，支持内置源和扩展源。其中内置源包括 mqtt、redis、neuron、memory、httppull、httppush、file、edgex,
扩展源包括 random、sql、video、zmq 以及用户自定义源
2. confKey: Config Key 名称。以上面为例，Config Key 依次为 amd_broker、default、demo_conf。


### 示例

删除 MQTT 源下名为 demo_conf 的 Config Key

```shell
 curl -X DELETE http://localhost:9081/metadata/sources/mqtt/confKeys/demo_conf
```

## 注册某个 configKey

该 API 用于在特定源名下注册 Config Key

```shell
PUT http://localhost:9081/metadata/sources/{name}/confKeys/{confKey}
```
### 参数

1. name：源名称，支持内置源和扩展源。其中内置源包括 mqtt、redis、neuron、memory、httppull、httppush、file、edgex,
   扩展源包括 random、sql、video、zmq 以及用户自定义源
2. confKey: 要注册的 Config Key 名称。


### 示例

在 MQTT 源下注册名为 demo_conf 的 Config Key

```shell
 curl -X PUT http://localhost:9081/metadata/sources/mqtt/confKeys/demo_conf
 {
   "demo_conf": {
		"qos": 0,
		"server": "tcp://10.211.55.6:1883"
	}
 }
```
