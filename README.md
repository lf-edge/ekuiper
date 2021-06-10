# EMQ X Kuiper - An edge lightweight IoT data analytics software

[![GitHub Release](https://img.shields.io/github/release/emqx/kuiper?color=brightgreen)](https://github.com/emqx/kuiper/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/kuiper)](https://hub.docker.com/r/emqx/kuiper)
[![Slack Invite](<https://kuiper-slack-invite.emqx.io/badge.svg>)](https://kuiper-slack-invite.emqx.io/)
[![Twitter](https://img.shields.io/badge/Follow-EMQ-1DA1F2?logo=twitter)](https://twitter.com/EMQTech)
[![Community](https://img.shields.io/badge/Community-Kuiper-yellow?logo=github)](https://github.com/emqx/kuiper/discussions)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ-FF0000?logo=youtube)](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q)

[English](README.md) | [简体中文](README-CN.md)

## Overview

EMQ X Kuiper is an edge lightweight IoT data analytics / streaming software implemented by Golang, and it can be run at all kinds of resource constrained edge devices. One goal of Kuiper is to migrate the cloud streaming software frameworks (such as [Apache Spark](https://spark.apache.org)，[Apache Storm](https://storm.apache.org) and [Apache Flink](https://flink.apache.org)) to edge side.  Kuiper references these cloud streaming frameworks, and also considered special requirement of edge analytics, and introduced **rule engine**, which is based on ``Source``, ``SQL (business logic)`` and ``Sink``, rule engine is used for developing streaming applications at edge side.

![arch](./docs/en_US/arch.png)

**User scenarios**

It can be run at various IoT edge use scenarios, such as real-time processing of production line data in the IIoT; Gateway of Connected Vehicle analyze the data from data-bus in real time; Real-time analysis of urban facility data in smart city scenarios. Kuiper processing at the edge can reduce system response latency, save network bandwidth and storage costs, and improve system security.

## Features

- Lightweight

  - Core server package is only about 4.5M, initial memory footprint is about 10MB

- Cross-platform

  - CPU Arch：X86 AMD * 32, X86 AMD * 64; ARM * 32, ARM * 64; PPC
  - The popular Linux distributions, OpenWrt Linux, MacOS and Docker
  - Industrial PC, Raspberry Pi, industrial gateway, home gateway, MEC edge cloud server

- Data analysis support

  - Support data extract, transform and filter through SQL 
  - Data order, group, aggregation and join
  - 60+ functions, includes mathematical, string, aggregate and hash etc
  - 4 time windows & count window

- Highly extensibile 

  Plugin system is provided,  and it supports to extend at ``Source``, ``SQL functions `` and ``Sink``.

  - Source: embedded support for MQTT, and provide extension points for sources
  - Sink: embedded support for MQTT and HTTP, and provide extension points for sinks
  - UDF functions: embedded support for 60+ functions, and provide extension points for SQL functions

- Management

  - [A web based management dashboard](https://hub.docker.com/r/emqx/kuiper-manager) for nodes, plugins, streams & rules management
  - Plugins, streams and rules management through CLI & REST API
  - Easily be integrate with [KubeEdge](https://github.com/kubeedge/kubeedge), [K3s](https://github.com/rancher/k3s) and [Baetyl](https://github.com/baetyl/baetyl), which bases Kubernetes

- Integration with EMQ X Edge

  Seamless integration with EMQ X Neuron & EMQ X Edge, and provided an end to end solution from messaging to analytics. 

## Quick start

- [Kuiper 5 minutes quick start](docs/en_US/quick_start_docker.md)
- [EdgeX rule engine tutorial](docs/en_US/edgex/edgex_rule_engine_tutorial.md)

## Slack channels
Join our [Slack](https://join.slack.com/t/lfedge/shared_invite/zt-7kavdtmq-SeyFzM2CEABBcKYGEVCgkw), and then join [ekuiper](https://lfedge.slack.com/archives/C024F4P7KCK) or [ekuiper-user](https://lfedge.slack.com/archives/C024F4SMEMR) channel.

## Performance test result

### MQTT throughput test

- Using JMeter MQTT plugin to send simulation data to EMQ X Broker, such as: ``{"temperature": 10, "humidity" : 90}``, the value of temperature and humidity are random integer between 0 - 100.
- Kuiper subscribe from EMQ X Broker, and analyze data with SQL: ``SELECT * FROM demo WHERE temperature > 50 `` 
- The analysis result are wrote to local file by using [file sink plugin](docs/en_US/plugins/sinks/file.md).

| Devices                                        | Message # per second | CPU usage     | Memory usage |
| ---------------------------------------------- | -------------------- | ------------- | ------------ |
| Raspberry Pi 3B+                               | 12k                  | sys+user: 70% | 20M          |
| AWS t2.micro( 1 Core * 1 GB) <br />Ubuntu18.04 | 10k                  | sys+user: 25% | 20M          |

### EdgeX throughput test

- A [Go application](fvt_scripts/edgex/benchmark/pub.go) is wrote to send data to ZeroMQ message bus, the data is as following.

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

- Kuiper subscribe from EdgeX ZeroMQ message bus, and analyze data with SQL: ``SELECT * FROM demo WHERE temperature > 50``. 90% of data will be filtered by the rule.

- The analysis result are sent to [nop sink](docs/en_US/rules/sinks/nop.md), all of the result data will be ignored.

|                                                | Message # per second | CPU usage     | Memory usage |
| ---------------------------------------------- | -------------------- | ------------- | ------------ |
| AWS t2.micro( 1 Core * 1 GB) <br />Ubuntu18.04 | 11.4 k               | sys+user: 75% | 32M          |

### Max number of rules support

- 8000 rules with 800 message/second
- Configurations
  - 2 core * 4GB memory in AWS
  - Ubuntu
- Resource usage
  - Memory: 89% ~ 72%
  - CPU: 25%
  - 400KB - 500KB / rule
- Rule
  - Source: MQTT
  - SQL: SELECT temperature FROM source WHERE temperature > 20 (90% data are filtered) 
  - Sink: Log

## Documents

- [Getting started](docs/en_US/getting_started.md) 

- [Reference guide](docs/en_US/reference.md)
  - [Install and operation](docs/en_US/operation/overview.md)
  - [Command line interface tools - CLI](docs/en_US/cli/overview.md)
  - [Kuiper SQL reference](docs/en_US/sqls/overview.md)
  - [Rules](docs/en_US/rules/overview.md)
  - [Extend Kuiper](docs/en_US/extension/overview.md)
  - [Plugins](docs/en_US/plugins/overview.md)

## Build from source

#### Preparation

- Go version >= 1.13

#### Compile

+ Binary: 

  - Binary: `$ make`

  - Binary files that support EdgeX: `$ make build_with_edgex`

+ Packages: `` $ make pkg``

  - Packages: `$ make pkg`

  - Packages files that support EdgeX: `$ make pkg_with_edgex`

+ Docker images: `$ make docker`

  > Docker images support EdgeX by default

To using cross-compilation, refer to [this doc](docs/en_US/cross-compile.md).

## Open source license

[Apache 2.0](LICENSE)
