# LF Edge eKuiper - 超轻量物联网边缘数据分析软件

LF Edge eKuiper 是 Golang 实现的轻量级物联网边缘分析、流式处理开源软件，可以运行在各类资源受限的边缘设备上。eKuiper 设计的一个主要目标就是将在云端运行的实时流式计算框架（比如 [Apache Spark](https://spark.apache.org)，[Apache Storm](https://storm.apache.org) 和 [Apache Flink](https://flink.apache.org) 等）迁移到边缘端。eKuiper 参考了上述云端流式处理项目的架构与实现，结合边缘流式数据处理的特点，采用了编写基于`源 (Source)`，`SQL (业务逻辑处理)`, `目标 (Sink)` 的规则引擎来实现边缘端的流式数据处理。

eKuiper 可以运行在各类物联网的边缘使用场景中，比如工业物联网中对生产线数据进行实时处理；车联网中的车机对来自汽车总线数据的即时分析；智能城市场景中，对来自于各类城市设施数据的实时分析。通过 eKuiper 在边缘端的处理，可以提升系统响应速度，节省网络带宽费用和存储成本，以及提高系统安全性等。

![arch](./resources/arch.png)

## 理解 eKuiper

了解 eKuiper 的基本概念。

- [eKuiper 是什么](./concepts/ekuiper.md)
- [流式处理概念](./concepts/streaming/overview.md)
- [规则及其组成](./concepts/rules.md)
- [源 Source](./concepts/sources/overview.md)
- [动作 Sink](./concepts/sinks.md)
- [基于 SQL 的规则逻辑](./concepts/sql.md)
- [扩展](./concepts/extensions.md)

[查看概念](./concepts/ekuiper.md)

## 快速上手

按照教程学习如何使用 eKuiper 。

- [本地快速开始](./getting_started.md)
- [使用 docker 快速开始](./quick_start_docker.md)
- [使用控制台界面](./operation/manager-ui/overview.md)
- [作为 EdgeX Foundry 规则引擎运行](./edgex/edgex_rule_engine_tutorial.md)
- [使用 OpenYurt 部署](./tutorials/deploy/openyurt_tutorial.md)

[查看教程](./tutorials/ai/tensorflow_lite_tutorial.md)

## 查阅参考信息

浏览语法和属性。

- [规则语法](./rules/overview.md)
- [可用源](./rules/sources/overview.md)
- [可用动作](./rules/sinks/overview.md)
- [可用函数](./sqls/built-in_functions.md)
- [SQL 语法](./sqls/overview.md)

[查看参考](./sqls/overview.md)

## 使用 eKuiper

了解如何创建和管理规则以及修改配置等运营知识。

- [配置](./operation/config/configuration_file.md)
- [Rest API](./operation/restapi/overview.md)
- [命令行](./operation/cli/overview.md)

[查看使用指南](./operation/overview.md)

## 开发扩展

了解如何开发自定义扩展。

- [原生 Go 插件开发](./extension/native/develop/overview.md)
- [Portable Go 插件开发](./extension/portable/go_sdk.md)
- [Python 插件开发](./extension/portable/python_sdk.md)

[查看扩展开发](./extension/overview.md)

## 获取帮助

如果您遇到问题，欢迎通过以下渠道寻求社区帮助：

- 创建 GitHub [issue](https://github.com/lf-edge/ekuiper/issues) 。
- [论坛](https://askemq.com/c/ekuiper)提问。
- 加入 [Slack](https://slack.lfedge.org/)的 [ekuiper](https://lfedge.slack.com/archives/C024F4P7KCK) 或者 [ekuiper-user](https://lfedge.slack.com/archives/C024F4SMEMR) 频道。
- 写邮件到 ekuiper [邮件组](mailto:ekuiper+help@lists.lfedge.org)。
- 加入微信群，扫描二维码，留言 "eKuiper"，工作人员将拉您进群。
  
  <img src="./wechat.png" alt="drawing" width="200"/>

## 贡献

任何人都可以参与项目，可通过贡献代码或者文档，回答问题等任何方式加入社区。

- [编辑文档](https://github.com/lf-edge/ekuiper/tree/master/docs)
- [如何参与](./CONTRIBUTING.md)