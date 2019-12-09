# `Dockerfile` links

- [emqx/kuiper](https://github.com/emqx/kuiper/blob/master/docker/Dockerfile)

# Quick reference

- **Where to get help**:

  **<u>Web</u>**

  - https://github.com/emqx/kuiper

  **<u>Documents</u>**

  - [Getting started](docs/en_US/getting_started.md) 

  - [Reference guide](docs/en_US/reference.md)
    - [Install and operation](docs/en_US/operation/overview.md)
    - [Command line interface tools - CLI](docs/en_US/cli/overview.md)
    - [Kuiper SQL reference](docs/en_US/sqls/overview.md)
    - [Rules](docs/en_US/rules/overview.md)
    - [Extend Kuiper](docs/en_US/extension/overview.md)
    - [Plugins](docs/en_US/plugins/overview.md)

- **Where to file issues:**

  https://github.com/emqx/kuiper/issues

- **Supported architectures**

  `amd64`, `arm64v8`,  `arm32v7`, `i386`, `ppc64le`

- **Supported Docker versions**:

  [The latest release](https://github.com/docker/docker-ce/releases/latest)

# Image Variants

The `emqx/kuiper` images come in many flavors, each designed for a specific operate systems.

## `emqx/kuiper:<tag>`

This is a stable release image that you can use with confidence.

## `emqx/kuiper:<tag>-<number>-<commit>`

This is an unstable version. It is an image built according to the commit number. You can use it to experience the latest features.


# What is Kuiper

EMQ X Kuiper is an edge lightweight IoT data analytics / streaming software implemented by Golang, and it can be run at all kinds of resource constrained edge devices. One goal of Kuiper is to migrate the cloud streaming software frameworks (such as [Apache Spark](https://spark.apache.org)，[Apache Storm](https://storm.apache.org) and [Apache Flink](https://flink.apache.org)) to edge side.  Kuiper references these cloud streaming frameworks, and also considered special requirement of edge analytics, and introduced **rule engine**, which is based on ``Source``, ``SQL (business logic)`` and ``Sink``, rule engine is used for developing streaming applications at edge side.

<!--TODO：an arch picture -->

**User scenarios**

It can be run at various IoT edge use scenarios, such as real-time processing of production line data in the IIoT; Gateway of Connected Vehicle analyze the data from data-bus in real time; Real-time analysis of urban facility data in smart city scenarios. Kuiper processing at the edge can reduce system response latency, save network bandwidth and storage costs, and improve system security.

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
  - Stream and rule management through CLI
  - Stream and rule management through REST API (In planning)
  - Easily be integrate with [KubeEdge](https://github.com/kubeedge/kubeedge) and [K3s](https://github.com/rancher/k3s), which bases Kubernetes
- Integration with EMQ X Edge
  Seamless integration with EMQ X Edge, and provided an end to end solution from messaging to analytics. 


# How to use this image

### Run kuiper

Execute some command under this docker image

```
docker run -d -v `pwd`:$somewhere emqx/kuiper:$tag $somecommand
```

For example

```
docker run -d --name kuiper -e MQTT_BROKER_ADDRESS=$MQTT_BROKER_ADDRESS emqx/kuiper:latest
```

### Configuration

Use the environment variable to configure `etc/sources/mqtt.yaml`  on the Kuiper container.

| Options                    | Default            | Mapped                    |
| ---------------------------| ------------------ | ------------------------- |
| MQTT_BROKER_ADDRESS         | 127.0.0.1:1883 | default.servers |
| MQTT_BROKER_SHARED_SUBSCRIPTION | true   | default.sharedSubscription |
| MQTT_BROKER_QOS | 1                 | default.qos    |
| MQTT_BROKER_USERNAME |   | default.username |
| MQTT_BROKER_PASSWORD |                | default.password |
| MQTT_BROKER_CER_PATH |                | default.certificationPath |
| MQTT_BROKER_KEY_PATH |     | default.privateKeyPath |

If you want to configure more options, you can mount the configuration file into Kuiper container.

# More

If you'd like to know more about the project, please refer to [Github project](https://github.com/emqx/kuiper/blob/master/docs/en_US/README.md).

