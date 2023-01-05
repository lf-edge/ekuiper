# LF Edge eKuiper - 超轻量物联网边缘流处理软件

LF Edge eKuiper 是 Golang 实现的轻量级物联网边缘分析、流式处理开源软件，可以运行在各类资源受限的边缘设备上。eKuiper的主要目标是在边缘端提供一个流媒体软件框架（类似于[Apache Flink](https://flink.apache.org)）。eKuiper的**规则引擎**允许用户提供基于SQL或基于图形（类似于Node-RED）的规则，在几分钟内创建物联网边缘分析应用。

![arch](./resources/arch.png)

## 功能

- 超轻量

  - 核心服务安装包约 4.5MB，首次运行内存使用约 10MB

- 跨平台

  - CPU 架构：X86 AMD * 32/64; ARM * 32/64; PPC
  - 常见 Linux 发行版、OpenWrt 嵌入式系统、MacOS、Docker
  - 工控机、树莓派、工业网关、家庭网关、MEC 边缘云等

- 完整的数据分析

  - 数据抽取、转换和过滤
  - 数据排序、分组、聚合、连接
  - 60+ 各类函数，覆盖数学运算、字符串处理、聚合运算和哈希运算等
  - 4 类时间窗口，以及计数窗口

- 高可扩展性

  可支持通过 Golang 或者 Python 在``源 (Source)``，``SQL 函数 ``, ``目标 (Sink)`` 三个方面的扩展

  - 源 (Source) ：允许用户接入更多的数据源用于数据分析
  - 目标(Sink)：允许用户将分析结果发送到不同的扩展系统中
  - SQL 函数：允许用户增加自定义函数用于数据分析（比如，AI/ML 的函数调用）

- 管理能力
  - [免费基于 Web 的管理控制台](https://hub.docker.com/r/emqx/ekuiper-manager)，用于可视化管理
  - 通过命令行、REST API 和 config map 对流、规则和插件进行管理
  - 与 [KubeEdge](https://github.com/kubeedge/kubeedge)、[OpenYurt](https://openyurt.io/)、[K3s](https://github.com/rancher/k3s)、[Baetyl](https://github.com/baetyl/baetyl) 等基于边缘 Kubernetes 框架的集成能力

- 与 EMQX 产品集成

  与 [EMQX](https://www.emqx.io/), [Neuron](https://neugates.io/) 和 [NanoMQ](https://nanomq.io/) 等产品无缝集成，为 IIoT 和 IoV 等提供端到端的解决方案

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

- [本地快速开始](./getting_started/getting_started.md)
- [使用 docker 快速开始](./getting_started/quick_start_docker.md)
- [使用控制台界面](./operation/manager-ui/overview.md)
- [作为 EdgeX Foundry 规则引擎运行](./edgex/edgex_rule_engine_tutorial.md)
- [使用 OpenYurt 部署](./integrations/deploy/openyurt_tutorial.md)

[查看教程](./guide/ai/tensorflow_lite_tutorial.md)

## 查阅参考信息

浏览语法和属性。

- [规则语法](./guide/rules/overview.md)
- [可用源](./guide/sources/overview.md)
- [可用动作](./guide/sinks/overview.md)
- [可用函数](./sqls/built-in_functions.md)
- [SQL 语法](./sqls/overview.md)

[查看参考](./sqls/overview.md)

## 使用 eKuiper

了解如何创建和管理规则以及修改配置等运营知识。

- [配置](./configuration/configuration.md)
- [Rest API](./api/restapi/overview.md)
- [命令行](./api/cli/overview.md)

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