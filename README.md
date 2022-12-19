# LF Edge eKuiper - An edge lightweight IoT data analytics software

[![GitHub Release](https://img.shields.io/github/release/lf-edge/ekuiper?color=brightgreen)](https://github.com/lf-edge/ekuiper/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/kuiper)](https://hub.docker.com/r/lfedge/ekuiper)
[![Slack](https://img.shields.io/badge/Slack-LF%20Edge-39AE85?logo=slack)](https://slack.lfedge.org/)
[![Twitter](https://img.shields.io/badge/Follow-EMQ-1DA1F2?logo=twitter)](https://twitter.com/EMQTech)
[![Community](https://img.shields.io/badge/Community-Kuiper-yellow?logo=github)](https://github.com/lf-edge/ekuiper/discussions)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ-FF0000?logo=youtube)](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q)

[English](README.md) | [简体中文](README-CN.md)

## Overview

LF Edge eKuiper is a lightweight IoT data analytics and stream processing engine running on resource-constraint edge devices. The major goal for eKuiper is to provide a streaming software framework (similar to [Apache Flink](https://flink.apache.org)) in edge side.  eKuiper's  **rule engine** allows user to provide either SQL based or graph based (similar to Node-RED) rules to create IoT edge analytics applications within few minutes.

![arch](./docs/en_US/resources/arch.png)

**User scenarios**

It can be run at various IoT edge user scenarios, such as,
- Real-time processing of production line data in the IIoT 
- Gateway of connected vehicle analyze the data from CAN in IoV
- Real-time analysis of wind turbines and smart bulk energy storage data in smart energy

eKuiper processing at the edge can greatly reduce system response latency, save network bandwidth and storage costs and improve system security.

## Features

- Lightweight

  - Core server package is only about 4.5M, memory footprint is about 10MB

- Cross-platform

  - CPU Arch：X86 AMD * 32/64; ARM * 32/64; PPC
  - Popular Linux distributions, OpenWrt Linux, MacOS and Docker
  - Industrial PC, Raspberry Pi, industrial gateway, home gateway, MEC edge cloud server

- Data analysis support

  - Support data ETL
  - Data order, group, aggregation and join with different data sources (the data from databases and files)
  - 60+ functions, includes mathematical, string, aggregate and hash etc
  - 4 time windows & count window

- Highly extensibile 

  It supports to extend at `Source`, `Functions` and `Sink` with Golang or Python.

  - Source: allows users to add more data source for analytics. 
  - Sink: allows users to send analysis result to different customized systems.
  - UDF functions: allow users to add customized functions for data analysis (for example, AI/ML function invocation) 

- Management

  - [A free web based management dashboard](https://hub.docker.com/r/emqx/ekuiper-manager) for visualized management
  - Plugins, streams and rules management through CLI, REST API and config maps(Kubernetes)
  - Easily be integrate with Kubernetes framworks [KubeEdge](https://github.com/kubeedge/kubeedge), [OpenYurt](https://openyurt.io/), [K3s](https://github.com/rancher/k3s) [Baetyl](https://github.com/baetyl/baetyl)

- Integration with EMQX products

  Seamless integration with [EMQX](https://www.emqx.io/), [Neuron](https://neugates.io/) & [NanoMQ](https://nanomq.io/), and provided an end to end solution from IIoT, IoV 

## Quick start

- [eKuiper 5 minutes quick start](docs/en_US/quick_start_docker.md)
- [EdgeX rule engine tutorial](docs/en_US/edgex/edgex_rule_engine_tutorial.md)

## Community

Join our [Slack](https://slack.lfedge.org/), and then join [ekuiper](https://lfedge.slack.com/archives/C024F4P7KCK) or [ekuiper-user](https://lfedge.slack.com/archives/C024F4SMEMR) channel.

### Meeting

Subscribe to community events [calendar](https://lists.lfedge.org/g/ekuiper-tsc/calendar?calstart=2021-08-06).

Weekly community meeting at Friday 10:30AM GMT+8:
- [Zoom meeting link](https://zoom.us/j/95097577087?pwd=azZaOXpXWmFoOXpqK293RFp0N1pydz09 )
- [Meeting minutes](https://wiki.lfedge.org/display/EKUIPER/Weekly+Development+Meeting)

### Contributing
Thank you for your contribution! Please refer to the [CONTRIBUTING.md](./docs/en_US/CONTRIBUTING.md) for more information.

## Performance test result

### MQTT throughput test

- Using JMeter MQTT plugin to send IoT data to [EMQX Broker](https://www.emqx.io/), such as: `{"temperature": 10, "humidity" : 90}`, the value of temperature and humidity are random integer between 0 - 100.
- eKuiper subscribe from EMQX Broker, and analyze data with SQL: `SELECT * FROM demo WHERE temperature > 50 ` 
- The analysis result are wrote to local file by using [file sink plugin](docs/en_US/rules/sinks/plugin/file.md).

| Devices                                        | Message # per second | CPU usage     | Memory usage |
| ---------------------------------------------- | -------------------- | ------------- | ------------ |
| Raspberry Pi 3B+                               | 12k                  | sys+user: 70% | 20M          |
| AWS t2.micro( 1 Core * 1 GB) <br />Ubuntu18.04 | 10k                  | sys+user: 25% | 20M          |

### EdgeX throughput test

- A [Go application](test/edgex/benchmark/pub.go) is wrote to send data to ZeroMQ message bus, the data is as following.

  ```
  {
    "Device": "demo", "Created": 000, …
    "readings": 
    [
       {"Name": "Temperature", value: "30", "Created":123 …},
       {"Name": "Humidity", value: "20", "Created":456 …}
    ]
  }
  ```

- eKuiper subscribe from EdgeX ZeroMQ message bus, and analyze data with SQL: ``SELECT * FROM demo WHERE temperature > 50``. 90% of data will be filtered by the rule.

- The analysis result are sent to [nop sink](docs/en_US/rules/sinks/builtin/nop.md), so all of the result data will be ignored.

|                                                | Message # per second | CPU usage     | Memory usage |
| ---------------------------------------------- | -------------------- | ------------- | ------------ |
| AWS t2.micro( 1 Core * 1 GB) <br />Ubuntu18.04 | 11.4 k               | sys+user: 75% | 32M          |

### Max number of rules support

- 8000 rules with 800 message/second in total
- Configurations
  - 2 core * 4GB memory in AWS
  - Ubuntu
- Resource usage
  - Memory: 89% ~ 72%
  - CPU: 25%
  - 400KB - 500KB / rule
- Rule
  - Source: MQTT
  - SQL: `SELECT temperature FROM source WHERE temperature > 20` (90% data are filtered) 
  - Sink: Log

### Multiple rules with shared source instance

- 300 rules with a shared MQTT stream instance.
  - 500 messages/second in the MQTT source
  - 150,000 message processing per second in total
- Configurations:
  - 2 Core * 2GB memory in AWS
  - Ubuntu
- Resource usage
  - Memory: 95MB
  - CPU: 50%
- Rule
  - Source: MQTT
  - SQL: `SELECT temperature FROM source WHERE temperature > 20`, (90% data are filtered)
  - Sink: 90% nop and 10% MQTT

To run the benchmark by yourself, please check [the instruction](./test/benchmark/multiple_rules/readme.md).

## Documents

Check out the [latest document](https://ekuiper.org/docs/en/latest/) in official website.


## Build from source

#### Preparation

- Go version >= 1.18

#### Compile

+ Binary: 

  - Binary: `$ make`

  - Binary files that support EdgeX: `$ make build_with_edgex`

  - Minimal binary file with core runtime only: `$ make build_core`

+ Packages: `` $ make pkg``

  - Packages: `$ make pkg`

  - Packages files that support EdgeX: `$ make pkg_with_edgex`

+ Docker images: `$ make docker`

  > Docker images support EdgeX by default

Prebuilt binaries are provided in the release assets. If using os or arch which does not have prebuilt binaries, please use cross-compilation, refer to [this doc](docs/en_US/operation/compile/cross-compile.md).

During compilation, features can be selected through go build tags so that users can build a customized product with only the desired feature set to reduce binary size. This is critical when the target deployment environment has resource constraint. Please refer to [features](docs/en_US/features.md) for more detail.

## Open source license

[Apache 2.0](LICENSE)
