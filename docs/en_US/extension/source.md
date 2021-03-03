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

The main task for a Source is to implement _open_ method. The implementation should be synchronized to create a connection to the external system. Then continuously receive data from the external system and send the received message to the consumer channel. The consumer channel accepts SourceTuple interface which is composed by a map for the message body and another map for the optional metadata. Typically, use `api.NewDefaultSourceTuple(message, meta)` to create a SourceTuple. The meta data could be anything that worth to be recorded. For example, the qualified topic of the message. The first parameter is a StreamContext pointer. You can retrieve the context information and logger etc. from it. It is also an implementation of go context, so you can listen to Done() channel to know if the parent stream has quit. For any errors happening during the connection or receiving, handle it in this method. If the error cannot be handled, send it to the errCh. By default, the rule will be terminated if any errors received from errCh.

```go
//Should be sync function for normal case. The container will run it in go func
Open(ctx StreamContext, consumer chan<- SourceTuple, errCh chan<- error)
```  

The last method to implement is _Close_ which literally close the connection. It is called when the stream is about to terminate. You could also do any clean up work in this function.

```go
Close(ctx StreamContext) error
```

As the source itself is a plugin, it must be in the main package. Given the source struct name is mySource. At last of the file, the source must be exported as a symbol as below. There are [2 types of exported symbol supported](overview.md#plugin-development). For source extension, states are usually needed, so it is recommended to export a constructor function.

```go
function MySource() api.Source{
    return &mySource{}
}
```

The [Randome Source](../../../plugins/sources/random.go) is a good example.

### Rewindable source
If the [rule checkpoint](../rules/state_and_fault_tolerance.md#source-consideration) is enabled, the source requires to be rewindable. That means the source need to implement both ``api.Source`` and ``api.Rewindable`` interface. 

A typical implementation is to save an ``offset`` as a field of the source. And update the offset value when reading in new value. Notice that, when implementing GetOffset() will be called by Kuiper system which means the offset value can be accessed by multiple go routines. So a lock is required when read or write the offset.



### Deal with configuration

Kuiper configurations are formatted as yaml and it provides a centralize location _/etc_ to hold all the configurations. Inside it, a subfolder _sources_ is provided for the source configurations including the extended sources.

A configuration system is supported for Kuiper extension which will automatically read the configuration in yaml file and feed into the _Configure_ method of the source. If the [CONF_KEY](../streams.md#create-stream) property is specified in the stream, the configuration of that key will be fed. Otherwise, the default configuration is used.
 
 To use configuration in your source, the following conventions must be followed.
 1. The name of your configuration file must be the same as the plugin name. For example, mySource.yaml.
 2. The yaml file must be located inside _etc/sources_
 3. The format of the yaml file could be found [here](../rules/sources/mqtt.md)
 
#### common configuration field

There are 2 common configuration fields.
 
* ``concurrency`` to specify how many instances will be started to run the source.
* ``bufferLength`` to specify the maximum number of messages to be buffered in the memory. This is used to avoid the extra large memory usage that would cause out of memory error. Notice that the memory usage will be varied to the actual buffer. Increase the length here won't increase the initial memory allocation so it is safe to set a large buffer length. The default value is 102400, that is if each payload size is about 100 bytes, the maximum buffer size will be about 102400 * 100B ~= 10MB.

### Package the source
Build the implemented source as a go plugin and make sure the output so file resides in the plugins/sources folder.

```bash
go build -trimpath --buildmode=plugin -o plugins/sources/MySource.so plugins/sources/my_source.go
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