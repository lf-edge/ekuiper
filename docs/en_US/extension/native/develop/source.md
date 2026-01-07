# Source Extension

Sources feed data into eKuiper from other systems. eKuiper has built-in source support for [MQTT broker](../../../guide/sources/builtin/mqtt.md). There are still needs to consume data from various external systems include messaging systems and data pipelines etc. Source extension is presented to meet this requirement.

**_Note_**: v2.0.0 has modified the source extension API, which is not fully compatible with the v1.x plugin API.
Existing plugin code needs to be re-adapted.

There are two kinds of sources. One is the normal source also named scan source, the other is the lookup source. A normal source can be used as a stream or scan table; A lookup source can be used as a lookup table. Users can develop one kind or both in a source plugin.

## Develop a scan source

To develop a source for eKuiper is to
implement [api.Source](https://github.com/lf-edge/ekuiper/blob/master/contract/api/source.go) interface and export it as
a golang plugin.

Before starting the development, you
must [setup the environment for golang plugin](./overview.md#setup-the-plugin-developing-environment).

Based on whether the data source is pulled on a schedule and whether the data is binary, sources can be categorized into
four types of interfaces:

- `ByteSource`: Push source, where the payload is binary data, and can be decoded with a configurable format, such as
  MQTT data source.
- `TupleSource`: Push source, where the payload is in a non-universal format and needs to be decoded by the plugin
  itself, such as Memory data source.
- `PullBytesSource`: Pull source, where the payload is in binary format and can be decoded with a configurable format,
  such as Video data source.
- `PullTupleSource`: Pull source, where the payload is in a non-universal format and needs to be decoded by the plugin
  itself, such as HttpPull data source.

To develop a source, first, you need to confirm which type of source the extension belongs to, and then implement the
corresponding type of methods.

### General Methods

1. To develop a source, the **Provision** method must be implemented. This method will be called once the source is
   initialized. In this method, you can retrieve the context of the rule to do logging etc. Then in the second
   parameter, a map that contains the configuration in your **yam** file is passed.
   See [configuration](#deal-with-configuration) for more detail. Typically, there will be information such as host,
   port, user and password of the external system. You can use this map to initialize this source.

   ```go
   //Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties read from the yaml
   Provision(ctx StreamContext, configs map[string]any) error
   ```

2. Implement the **Connect** method. This method is used to initialize and establish a connection with the external
   system and is executed only once during rule initialization. The second parameter is used to pass the long-connection
   status to the rule. For example, if the connection implementation automatically reconnects, the reconnection logic
   should run asynchronously to avoid blocking the rule's execution. When the connection logic becomes asynchronous,
   changes in the connection status can be notified to the rule by calling the state change callback function.

   ```go
   Connect(ctx StreamContext, sch StatusChangeHandler) error
   ```

3. Implement the subscription or pull method for the source type. This is the main execution logic of the source, used
   to fetch data from the external system and send it to the eKuiper system for consumption by downstream operators. The
   methods implemented by different types of sources vary slightly. For more details, please refer
   to [Source Type Implementation](#various-source-type-implementation).

4. The last method to implement is **Close**, which is actually used to close the connection. It is called when the
   stream is about to terminate. You can also perform any cleanup work in this function.

   ```go
   Close(ctx StreamContext) error
   ```

5. As the source itself is a plugin, it must be in the main package. Given the source struct name is mySource. At last
   of the file, the source must be exported as a symbol as below. There
   are [2 types of exported symbol supported](./overview.md#plugin-development). For source extension, states are
   usually needed, so it is recommended to export a constructor function.

   ```go
   function MySource() api.Source{
       return &mySource{}
   }
   ```

The [Random Source](https://github.com/lf-edge/ekuiper/blob/master/extensions/sources/random/random.go) is a good example.

### Various Source Type Implementation

The main task of a source is to continuously receive data from an external system and read it into the system.

- `ByteSource`: Needs to implement the `Subscribe` method, used to subscribe to data changes (receive data pushed by the
  external system). Call `BytesIngest` to consume the subscribed data and `ErrorIngest` to send error information. Refer
  to the MQTT source implementation, subscribe to the configured topics, and read the subscribed bytes data through the
  ingest method.

  ```go
  Subscribe(ctx StreamContext, ingest BytesIngest, ingestError ErrorIngest) error
  ```

- `TupleSource`: Needs to implement the `Subscribe` method, used to subscribe to data changes (receive data pushed by
  the external system). Call `TupleIngest` to consume the subscribed and decoded map data; call `ErrorIngest` to send
  error information. Refer to the Memory source implementation.

  ```go
  Subscribe(ctx StreamContext, ingest TupleIngest, ingestError ErrorIngest) error
  ```

- `PullBytesSource`: Needs to implement the `Pull` method, used to pull data. The pull interval can be configured via
  the `interval` parameter. Call `BytesIngest` to consume the pulled data, call `ErrorIngest` to send error information,
  and `trigger` is the time of this pull. Refer to the Video data source implementation.

  ```go
  Pull(ctx StreamContext, trigger time.Time, ingest BytesIngest, ingestError ErrorIngest)
  ```

- `PullTupleSource`: Needs to implement the `Pull` method, used to pull data. The pull interval can be configured via
  the `interval` parameter. Call `TupleIngest` to consume the pulled and decoded map data, call `ErrorIngest` to send
  error information, and `trigger` is the time of this pull. Refer to the HttpPull data source implementation.

  ```go
  Pull(ctx StreamContext, trigger time.Time, ingest TupleIngest, ingestError ErrorIngest)
  ```

### Develop a lookup source

To develop a lookup source for eKuiper is to implement [api.LookupSource](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) interface and export it.

Based on whether the data from the data source is binary, sources can be categorized into two types of interfaces:

- `LookupBytesSource`
- `LookupSource`

Users need to choose one interface to implement based on the actual type of extension. The lookup source, like a regular
data source, needs to implement [general methods](#general-methods). Then, implement the `Lookup` method.

The main task of the lookup source is to implement the **Lookup** method. This method will run in each join operation.
The parameters are obtained at runtime, including fields, keys, and values to be retrieved from the external system.
Each lookup source has a different query mechanism. For example, an SQL lookup source will assemble an SQL query from
these parameters to retrieve the query data.

Depending on the type of Payload, the `Lookup` methods of the two interfaces vary slightly.

- `LookupSource`: The plugin implements decoding, and the return value is a list of maps.

  ```go
  Lookup(ctx StreamContext, fields []string, keys []string, values []any) ([]map[string]any, error)
  ```

- `LookupBytesSource`: The plugin returns the raw binary data, which is automatically decoded by the eKuiper framework
  based on the `format` parameter.

  ```go
  Lookup(ctx StreamContext, fields []string, keys []string, values []any) ([][]byte, error)
  ```

The [SQL Lookup Source](https://github.com/lf-edge/ekuiper/blob/master/extensions/sources/sql/sqlLookup.go) is a good example.

## Source Traits

In addition to the data ingestion functionality in the default interface, extended sources can optionally implement
feature interfaces to support various source features.

### Rewindable source

If the [rule checkpoint](../../../guide/rules/state_and_fault_tolerance.md#source-consideration) is enabled, the source requires to be rewindable. That means the source need to implement both `api.Source` and `api.Rewindable` interface.

A typical implementation is to save an `offset` as a field of the source. And update the offset value when reading in new value. Notice that, when implementing GetOffset() will be called by eKuiper system which means the offset value can be accessed by multiple go routines. So a lock is required when read or write the offset.

### Bounded Source

Some data sources are bounded, such as files; some data sources are inherently unbounded, but in some scenarios, users
may want to stop reading after a certain amount of data. eKuiper supports data sources to define their own read-end
signals. When the framework receives the data end signal, the corresponding rule will stop on its own.

Data sources can implement the `api.Bounded` interface to obtain the `EOFIngest` method. After the data reading is
completed, call this method to notify the framework that the data reading is finished. The File Source is a built-in
bounded source, and its implementation can be referenced during development.

## Configuration and Usage

eKuiper configurations are formatted as yaml and it provides a centralize location **/etc** to hold all the
configurations. Inside it, a subfolder **sources** is provided for the source configurations including the extended
sources.

A configuration system is supported for eKuiper extension which will automatically read the configuration in yaml file
and feed into the **Provision** method of the source. If
the [CONF_KEY](../../../guide/streams/overview.md#stream-properties) property is specified in the stream, the
configuration of that key will be fed. Otherwise, the default configuration is used.

To use configuration in your source, the following conventions must be followed.

1. The name of your configuration file must be the same as the plugin name. For example, mySource.yaml.
2. The yaml file must be located inside **etc/sources**
3. The format of the yaml file could be found [here](../../../guide/sources/builtin/mqtt.md)

### common configuration field

There are 2 common configuration fields.

- `interval`: If the data source is of the pull type, this parameter specifies the interval for pulling. If it is a push
  source, this parameter is not configured by default, and the data source is data-triggered; if configured, this
  parameter defines the frequency of pushing.
- `bufferLength` to specify the maximum number of messages to be buffered in the memory. This is used to avoid the extra
  large memory usage that would cause out of memory error. Notice that the memory usage will be varied to the actual
  buffer. Increase the length here won't increase the initial memory allocation so it is safe to set a large buffer
  length. The default value is 102400, that is if each payload size is about 100 bytes, the maximum buffer size will be
  about 102400 \* 100B ~= 10MB.

### Usage

The customized source is specified in a [stream definition](../../../guide/streams/overview.md#stream-properties). The related properties are:

- TYPE: specify the name of the source, must be camel case.
- CONF_KEY: specify the key of the configuration to be used.

If you have developed a source implementation MySource, you should have:

1. In the plugin file, symbol MySource is exported.
2. The compiled MySource.so file is located inside **plugins/sources**
3. If configuration needed, put mySource.yaml inside **etc/sources**

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
