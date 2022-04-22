# LF Edge eKuiper - An edge lightweight IoT data analytics software

eKuiper is an open source stream processing engine for edge computing. The open source project is hosted by [LF Edge](https://lfedge.org/).

It can be run at all kinds of resource constrained edge devices. One goal of eKuiper is to migrate the cloud streaming software frameworks (such as [Apache Spark](https://spark.apache.org)，[Apache Storm](https://storm.apache.org) and [Apache Flink](https://flink.apache.org)) to edge side.  eKuiper references these cloud streaming frameworks, and also considered special requirement of edge analytics, and introduced **rule engine**, which is based on `Source`, `SQL (business logic)` and `Sink`, rule engine is used for developing streaming applications at edge side.

![arch](./resources/arch.png)

## Understand eKuiper

Learn about eKuiper and its fundamental concepts.

- [Why eKuiper](./concepts/ekuiper.md)
- [Stream Processing Concept](./concepts/streaming/overview.md)
- [Rule Composition](./concepts/rules.md)
- [Source](./concepts/sources/overview.md)
- [Sink](./concepts/sinks.md)
- [Rule logic by SQL](./concepts/sql.md)
- [Extension](./concepts/extensions.md)

[View Concepts](./concepts/ekuiper.md)

## Try eKuiper

Follow tutorials to learn how to use eKuiper.

- [Getting started locally](./getting_started.md)
- [Getting started in Docker](./quick_start_docker.md)
- [Getting started by dashboard](./operation/manager-ui/overview.md)
- [Run as EdgeX Foundry rule engine](./edgex/edgex_rule_engine_tutorial.md)
- [Deploy by OpenYurt](./tutorials/deploy/openyurt_tutorial.md)

[View Tutorials](./tutorials/ai/tensorflow_lite_tutorial.md)

## Look up reference information

Refer to the syntax and properties.

- [Rule Syntax](./rules/overview.md)
- [Available Sources](./rules/sources/overview.md)
- [Available Sinks](./rules/sinks/overview.md)
- [Available Functions](./sqls/built-in_functions.md)
- [SQL Reference](./sqls/overview.md)

[View Reference](./sqls/overview.md)

## Learn how to use eKuiper

Learn how to create and manage rules and how to modify configurations etc.

- [Configuration](./operation/config/configuration_file.md)
- [Rest API](./operation/restapi/overview.md)
- [CLI](./operation/cli/overview.md)

[View Operations](./operation/overview.md)

## Develop extensions

Learn how to develop custom extensions.

- [Native go plugin development](./extension/native/develop/overview.md)
- [Portable go plugin development](./extension/portable/go_sdk.md)
- [Python plugin development](./extension/portable/python_sdk.md)

[View Extension Programming](./extension/overview.md)

## Get Help

If you get stuck, check out our community support resources.

- Open GitHub [issue](https://github.com/lf-edge/ekuiper/issues).
- Ask in the [forum](https://askemq.com/c/ekuiper).
- Join our [Slack](https://slack.lfedge.org/), and then join [ekuiper](https://lfedge.slack.com/archives/C024F4P7KCK) or [ekuiper-user](https://lfedge.slack.com/archives/C024F4SMEMR) channel.
- Mail to eKuiper help [mail list](mailto:ekuiper+help@lists.lfedge.org).
- Join WeChat group, scan the below QR code and mark "eKuiper". 
  
  <img src="./wechat.png" alt="drawing" width="200"/>

## Contribute

Anyone can contribute for anything, not just code.

- [Edit Doc in GitHub](https://github.com/lf-edge/ekuiper/tree/master/docs)
- [How to contribute](./CONTRIBUTING.md)