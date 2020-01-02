# EMQ X Kuiper - An edge lightweight IoT data analytics software

[English](README.md) | [简体中文](README-CN.md)

## Overview

EMQ X Kuiper is an edge lightweight IoT data analytics / streaming software implemented by Golang, and it can be run at all kinds of resource constrained edge devices. One goal of Kuiper is to migrate the cloud streaming software frameworks (such as [Apache Spark](https://spark.apache.org)，[Apache Storm](https://storm.apache.org) and [Apache Flink](https://flink.apache.org)) to edge side.  Kuiper references these cloud streaming frameworks, and also considered special requirement of edge analytics, and introduced **rule engine**, which is based on ``Source``, ``SQL (business logic)`` and ``Sink``, rule engine is used for developing streaming applications at edge side.

![arch](docs/resources/arch.png)

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
  - 4 time windows

- Highly extensibile 

  Plugin system is provided,  and it supports to extend at ``Source``, ``SQL functions `` and ``Sink``.

  - Source: embedded support for MQTT, and provide extension points for sources
  - Sink: embedded support for MQTT and HTTP, and provide extension points for sinks
  - UDF functions: embedded support for 60+ functions, and provide extension points for SQL functions

- Management

  - Stream and rule management through CLI
  - Stream and rule management through REST API (In planning)
  - Easily be integrate with [KubeEdge](https://github.com/kubeedge/kubeedge) and [K3s](https://github.com/rancher/k3s), which bases Kubernetes

- Integration with EMQ X Edge

  Seamless integration with EMQ X Edge, and provided an end to end solution from messaging to analytics. 

## Performance test result

### Test scenario

- Using JMeter MQTT plugin to send simulation data to EMQ X Broker, such as: ``{"temperature": 10, "humidity" : 90}``, the value of temperature and humidity are random integer between 0 - 100.
- Kuiper subscribe from EMQ X Broker, and analyze data with SQL: ``SELECT * FROM demo WHERE temperature > 50 `` 
- The analysis result are wrote to local file by using [file sink plugin](docs/en_US/plugins/sinks/file.md).

| Devices                                        | Message # per second | CPU usage     | Memory usage |
| ---------------------------------------------- | -------------------- | ------------- | ------------ |
| Raspberry Pi 3B+                               | 12k                  | sys+user: 70% | 20M          |
| AWS t2.micro( 1 Core * 1 GB) <br />Ubuntu18.04 | 10k                  | sys+user: 25% | 20M          |

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

- Go version >= 1.11

#### Compile

- Binary: ``$ make``
- Packages: `` $ make pkg``
- Docker images: ``$ make docker``

To using cross-compilation, refer to [this doc](docs/en_US/cross-compile.md).

## Open source license

[Apache 2.0](LICENSE)
