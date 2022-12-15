# `Dockerfile` links

- [lfedge/ekuiper](https://github.com/lf-edge/ekuiper/blob/master/docker/Dockerfile)

# Quick reference

- **Where to get help**:

  **<u>Website</u>**

  - https://ekuiper.org

  **<u>Github</u>**

  - https://github.com/lf-edge/ekuiper

  **<u>Documents</u>**

  - [Getting started](https://ekuiper.org/docs/en/latest/quick_start_docker.html) 
  - [Reference guide](https://ekuiper.org/docs/en/latest/)

- **Where to file issues:**

  https://github.com/lf-edge/ekuiper/issues

- **Supported architectures**

  `amd64`, `arm64v8`,  `arm32v7`, `i386`

- **Supported Docker versions**:

  [The latest release](https://github.com/docker/docker-ce/releases/latest)

# Image Variants

The `lfedge/ekuiper` images come in many flavors, each designed for a specific use case.

## `lfedge/ekuiper:<tag>`

This is the defacto image. If you are unsure about what your needs are, you probably want to use this one. It is an alias of tag `lfedge/ekuiper:<tag>-alpine`(see below) since 1.7.1. Previously, it is equivalent to the `lfedge/ekuiper:<tag>-dev` image

## `lfedge/ekuiper:<tag>-alpine`

This image is based on the popular [Alpine Linux project](http://alpinelinux.org), available in [the `alpine` official image](https://hub.docker.com/_/alpine). Alpine Linux is much smaller than most distribution base images (~5MB), and thus leads to much slimmer images in general.

This variant is highly recommended when final image size being as  small as possible is desired. The main caveat to note is that it does  use [musl libc](http://www.musl-libc.org) instead of [glibc and friends](http://www.etalabs.net/compare_libcs.html), so certain software might run into issues depending on the depth of  their libc requirements. However, most software doesn't have an issue  with this, so this variant is usually a very safe choice. See [this Hacker News comment thread](https://news.ycombinator.com/item?id=10782897) for more discussion of the issues that might arise and some pro/con comparisons of using Alpine-based images.

To minimize image size, it's uncommon for additional related tools (such as `git` or `bash`) to be included in Alpine-based images. Using this image as a base, add the things you need in your own Dockerfile (see the [`alpine` image description](https://hub.docker.com/_/alpine/) for examples of how to install packages if you are unfamiliar).

## `lfedge/ekuiper:<tag>-slim`

This image is based on Debian, and only contains the minimal packages needed to run eKuiper. The difference between this and dev image (`lfedge/ekuiper:<tag>-dev`) is that this image does not include Golang development environment. The typical usage of this image would be deploy the plugins compiled in previous Docker image instances. This is the official recommended image if you want to deploy & run  customized plugins into eKuiper.

## `lfedge/ekuiper:<tag>-slim-python`

This image is the same as slim except that it also contains python environment. It is recommended if using eKuiper python portable plugins.

## `lfedge/ekuiper:<tag>-dev`

This is the development Docker image, which is based on Debian and it also includes a Golang build environment. If you are unsure about what your needs  are, you probably want to use this one. It is designed to be used both as a throw away container (mount your source code, compile plugins for eKuiper,  and start the  container to run your app), as well as the base to build other images. Please be aware of that this image is the biggest size, and it's usually used for development purpose.

Notice: This image is the equivalent to development image of `x.x.x-dev` in 0.3.x versions.

# What is eKuiper

LF Edge eKuiper is an edge lightweight IoT data analytics / streaming software implemented by Golang, and it can be run at all kinds of resource constrained edge devices. One goal of eKuiper is to migrate the cloud streaming software frameworks (such as [Apache Spark](https://spark.apache.org)，[Apache Storm](https://storm.apache.org) and [Apache Flink](https://flink.apache.org)) to edge side.  eKuiper references these cloud streaming frameworks, and also considered special requirement of edge analytics, and introduced **rule engine**, which is based on ``Source``, ``SQL (business logic)`` and ``Sink``, rule engine is used for developing streaming applications at edge side.

![eKuiper architect](https://ekuiper.org/docs/docs-assets/img/arch.349f5fae.png)

**User scenarios**

It can be run at various IoT edge use scenarios, such as real-time processing of production line data in the IIoT; Gateway of Connected Vehicle analyze the data from data-bus in real time; Real-time analysis of urban facility data in smart city scenarios. eKuiper processing at the edge can reduce system response latency, save network bandwidth and storage costs, and improve system security.

**Features**

- Lightweight

  - Core server package is only about 3MB, initial memory usage is about 10MB
- Cross-platform
  - CPU Arch：X86 AMD * 32, X86 AMD * 64; ARM * 32, ARM * 64; PPC
  - The popular Linux distributions, MacOS and Docker
  - Industrial PC, Raspberry Pi, industrial gateway, home gateway, MEC edge cloud server
- Data analysis support
  - Support data extract, transform and filter through SQL 
  - Data order, group, aggregation and join
  - 60+ functions, includes mathematical, string, aggregate and hash etc
  - 4 time windows
- Highly extensibile

  Plugin system is provided,  and it supports to extend at Source, SQL functions and Sink.
  - Source: embedded support for MQTT, and provide extension points for sources
  - Sink: embedded support for MQTT and HTTP, and provide extension points for sinks
  - UDF functions: embedded support for 60+ functions, and provide extension points for SQL functions
- Management
  - [A web based management dashboard](https://hub.docker.com/r/emqx/ekuiper-manager) for nodes, plugins, streams & rules management
  - Plugins, streams and rules management through CLI & REST API
  - Easily be integrate with [KubeEdge](https://github.com/kubeedge/kubeedge), [K3s](https://github.com/rancher/k3s) and [Baetyl](https://github.com/baetyl/baetyl), which bases Kubernetes
- Integration with EMQ X Nuron & Edge
  Seamless integration with EMQ X Neuron & Edge, and provided an end to end solution from messaging to analytics. 


# How to use this image

### Run eKuiper

Execute some command under this docker image

```shell
docker run -d -v `pwd`:$somewhere lfedge/ekuiper:$tag $somecommand
```

For example

```shell
docker run -p 9081:9081 -d --name ekuiper MQTT_SOURCE__DEFAULT__SERVER="$MQTT_BROKER_ADDRESS" lfedge/ekuiper:$tag
```

> Notice that, before v1.5.0 the mqtt server property is an array. Please check [migration guide](#migration-guide) for detail.

#### 5 minutes quick start

1. Set eKuiper source to an MQTT server. This sample uses server locating at ``tcp://broker.emqx.io:1883``. ``broker.emqx.io`` is a public MQTT test server hosted by [EMQ](https://www.emqx.io).

   ```shell
   docker run -p 9081:9081 -d --name ekuiper -e MQTT_SOURCE__DEFAULT__SERVER="tcp://broker.emqx.io:1883" lfedge/ekuiper:$tag
   ```

2. Create a stream - the stream is your stream data schema, similar to table definition in database. Let's say the temperature & humidity data are sent to ``broker.emqx.io``, and those data will be processed in your **LOCAL RUN** eKuiper docker instance.  Below steps will create a stream named ``demo``, and data are sent to ``devices/device_001/messages`` topic, while ``device_001`` could be other devices, such as ``device_002``, all of those data will be subscribed and handled by ``demo`` stream.

   ```shell
   -- In host
   # docker exec -it ekuiper /bin/sh
   
   -- In docker instance
   # bin/kuiper create stream demo '(temperature float, humidity bigint) WITH (FORMAT="JSON", DATASOURCE="devices/+/messages")'
   Connecting to 127.0.0.1:20498...
   Stream demo is created.
   
   # bin/kuiper query
   Connecting to 127.0.0.1:20498...
   kuiper > select * from demo where temperature > 30;
   Query was submit successfully.
   
   ```

3. Publish sensor data to topic `devices/device_001/messages` of server `tcp://broker.emqx.io:1883` with any MQTT client such as [MQTT X](https://mqttx.app/).

   ```shell
   # mqttx pub -h broker.emqx.io -m '{"temperature": 40, "humidity" : 20}' -t devices/device_001/messages
   ```

4. If everything goes well,  you can see the message is print on docker ``bin/kuiper query`` window. Please try to publish another message with ``temperature`` less than 30, and it will be filtered by WHERE condition of the SQL. 

   ```
   kuiper > select * from demo WHERE temperature > 30;
   [{"temperature": 40, "humidity" : 20}]
   ```

   If having any problems, please take a look at ``log/stream.log``.

5. To stop the test, just press ``ctrl + c `` in ``bin/kuiper query`` command console.

You can also refer to [eKuiper dashboard documentation](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/operation/manager-ui/overview.md) for better using experience.

Next for exploring more powerful features of eKuiper? Refer to below for how to apply LF Edge eKuiper in edge and integrate with AWS / Azure IoT cloud.

- [Lightweight edge computing eKuiper and Azure IoT Hub integration solution](https://www.emqx.com/en/blog/lightweight-edge-computing-emqx-kuiper-and-azure-iot-hub-integration-solution)
- [Lightweight edge computing eKuiper and AWS IoT Hub integration solution](https://www.emqx.com/en/blog/lightweight-edge-computing-emqx-kuiper-and-aws-iot-hub-integration-solution)

### Configuration

eKuiper supports to use environment variables to modify configuration files in containers.

When modifying configuration files through environment variables, the environment variables need to be set according to the prescribed format, for example:

```
KUIPER__BASIC__DEBUG => basic.debug in etc/kuiper.yaml
MQTT_SOURCE__DEMO_CONF__QOS => demo_conf.qos in etc/mqtt_source.yaml
```

The environment variables are separated by two "_", the content of the first part after the separation matches the file name of the configuration file, and the remaining content matches the different levels of the configuration item.


### Migration Guide

Since 1.5.0, eKuiper changes the mqtt source broker configuration from `servers` to `server` and users can only configure a mqtt broker address instead of address array.
Users who are using mqtt broker as stream source in previous release and want to migrate to 1.5.0 release or later, need make sure the ``etc/mqtt_source.yaml`` file ``server`` 's configuration is right.
Users who are using environment variable to configure the mqtt source address need change their ENV successfully, for example, their broker address is ``tcp://broker.emqx.io:1883``. They need change the ENV from
``MQTT_SOURCE__DEFAULT__SERVERS=[tcp://broker.emqx.io:1883]`` to ``MQTT_SOURCE__DEFAULT__SERVER="tcp://broker.emqx.io:1883"``

### About EdgeX

Regarding the configuration content of `etc/sources/edgex.yaml`, these values are configured in the cmd `cmd/core-data/res/configuration.toml` of the EdgeX core data service, as shown below.

```
[MessageQueue]
Protocol = 'tcp'
Host = '*'
Port = 5563
Type = 'zero'
Topic = 'events'
```

```
[Service]
...
Host = 'localhost'
Port = 48080
...
```

If you want to configure more options, you can mount the configuration file into eKuiper container, like this:
```
$ docker run --name ekuiper -v /path/to/mqtt_sources.yaml:/kuiper/etc/mqtt_sources.yaml -v /path/to/edgex.yaml:/kuiper/etc/sources/edgex.yaml lfedge/ekuiper:$tag
```

# More

If you'd like to know more about the project, please refer to [Github project](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/README.md).

