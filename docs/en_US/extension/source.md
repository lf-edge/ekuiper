# Source Extension

Sources feed data into Kuiper from other systems. Kuiper has built-in source support for [MQTT broker](../rules/sources/mqtt.md). There are still needs to consume data from various external systems include messaging systems and data pipelines etc. Source extension is presented to meet this requirement.

## Developing

### Develop a source

To develop a source for Kuiper is to implement [api.Source](../../../xstream/api/stream.go) interface and export it as a golang plugin.

Before starting the development, you must [setup the environment for golang plugin](overview.md#setup-the-plugin-developing-environment). 

To develop a source, the _Configure_ method must be implemented. This method will be called once the source is initialized. In this method, you can retrieve the _DATASOURCE_ property of the stream (which is topic for mqtt and other messaging system) from the first parameter. Then in the second parameter, a map that contains the configuration in your _yaml_ file is passed. See [configuration](#deal-with-configuration) for more detail. Typically, there will be information such as host, port, user and password of the external system. You can use this map to initialize this source.

```go
//Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties read from the yaml 
Configure(datasource string, props map[string]interface{}) error
```

The main task for a Source is to implement _open_ method. The implementation should be synchronized to create a connection to the external system. Then run in a separate go routine to continuously receive data from the external system and call the consume function provided as the second parameter for each received message. The consume function accepts a map for the message body and another map for the optional metadata. The meta data could be anything that worth to be recorded. For example, the qualified topic of the message. The first parameter is a StreamContext pointer. You can retrieve the context information and logger etc. from it. It is also an implementation of go context, so you can listen to Done() channel to know if the parent stream has quit.

```go
//The function to call when data is emitted by the source.
type ConsumeFunc func(message map[string]interface{}, metadata map[string]interface{})

//Should be sync function for normal case. The container will run it in go func
Open(ctx StreamContext, consume ConsumeFunc) error
```  

The last method to implement is _Close_ which literally close the connection. It is called when the stream is about to terminate. You could also do any clean up work in this function.

```go
Close(ctx StreamContext) error
```

As the source itself is a plugin, it must be in the main package. Given the source struct name is mySource. At last of the file, the source must be exported as a symbol as below.

```go
var MySource mySource
```

The [Randome Source](../../../plugins/sources/random.go) is a good example.

### Deal with configuration

Kuiper configurations are formatted as yaml and it provides a centralize location _/etc_ to hold all the configurations. Inside it, a subfolder _sources_ is provided for the source configurations including the extended sources.

A configuration system is supported for Kuiper extension which will automatically read the configuration in yaml file and feed into the _Configure_ method of the source. If the [CONF_KEY](../streams.md#create-stream) property is specified in the stream, the configuration of that key will be fed. Otherwise, the default configuration is used.
 
 To use configuration in your source, the following conventions must be followed.
 1. The name of your configuration file must be the same as the _.so_ file and must be camel case with upper case first letter. For example, MySource.yaml.
 2. The yaml file must be located inside _etc/sources_
 3. The format of the yaml file could be found [here](../rules/sources/mqtt.md)
 
#### common configuration field

There is a common configuration field ``concurrency`` to specify how many instances will be started to run the source.

### Package the source
Build the implemented source as a go plugin and make sure the output so file resides in the plugins/sources folder.

```bash
go build --buildmode=plugin -o plugins/sources/MySource.so plugins/sources/my_source.go
```

### Usage

The customized source is specified in a [stream definition](../streams.md#create-stream). The related properties are:

- TYPE: specify the name of the source, must be camel case.
- CONF_KEY: specify the key of the configuration to be used.

If you have developed a source implementation MySource, you should have:
1. In the plugin file, symbol MySource is exported.
2. The compiled MySource.so file is located inside _plugins/sources_
3. If configuration needed, put mySource.yaml inside _etc/sources_

To use it, define a stream:
```sql
CREATE STREAM demo (
		USERID BIGINT,
		FIRST_NAME STRING,
		LAST_NAME STRING,
		NICKNAMES ARRAY(STRING),
		Gender BOOLEAN,
		ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT),
	) WITH (DATASOURCE="mytopic", TYPE="mySource", CONF_KEY="democonf");
```