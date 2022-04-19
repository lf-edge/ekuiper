# Available Sinks

In the eKuiper source code, there are built-in sinks and sinks in extension.

## Built-in Sinks

Users can directly use the built-in sinks in the standard eKuiper instance. The list of built-in sinks are:

- [Mqtt sink](./builtin/mqtt.md): sink to external mqtt broker.
- [Neuron sink](./builtin/neuron.md): sink to the local neuron instance.
- [EdgeX sink](./builtin/edgex.md): sink to EdgeX Foundry. This sink only exist when enabling edgex build tag.
- [Rest sink](./builtin/rest.md): sink to external http server.
- [Memory sink](./builtin/memory.md): sink to eKuiper memory topic to form rule pipelines.
- [Log sink](./builtin/log.md): sink to log, usually for debug only.
- [Nop sink](./builtin/nop.md): sink to nowhere. It is used for performance testing now.

## Predefined Sink Plugins

We have developed some official sink plugins. These plugins can be found in eKuiper's source code and users need to build them manually. Please check each sink about how to build and use.

Additionally, these plugins have pre-built binaries for the mainstream cpu architecture such as AMD or ARM. The pre-built plugin hosted in `https://packages.emqx.net/kuiper-plugins/$version/$os/sinks/$type_$arch.zip`. For example, to get tdengine sink for debian amd64, install it from `https://packages.emqx.net/kuiper-plugins/1.4.4/debian/sinks/tdengine_amd64.zip`.

The list of predefined sink plugins:

- [Zero MQ sink](./plugin/zmq.md): sink to zero mq.
- [File sink](./plugin/file.md): sink to a file.
- [InfluxDB sink](./plugin/influx.md): sink to influx db.
- [Tdengine sink](./plugin/tdengine.md): sink to tdengine.
- [Redis sink](./plugin/redis.md): sink to redis.
- [Image sink](./plugin/image.md): sink to an image file. Only used to handle binary result.