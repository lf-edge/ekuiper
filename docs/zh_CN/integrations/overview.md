# 集成

除丰富的数据处理功能外，eKuiper 还支持与各大平台的无缝集成。本章将介绍 eKuiper 与各生态系统的集成。

- **[云边协同](./edge_cloud/overview.md)**：本节将介绍如何基于 eKuiper 等开源产品建立一个边云协作解决方案，应用于 IIoT（工业物联网）、IoV（车联网）和其他物联网应用，如智慧城市等。
- **[Edgex Foundry](../edgex/edgex_rule_engine_tutorial.md)**：作为 Edgex Foundry 的实时流处理模块，eKuiper 与 Edgex Foundry 的集成进一步提升该物联网边缘计算开源平台的模块功能。
- **[Neuron](./neuron/neuron_integration_tutorial.md)**：eKuiper 与 Neuron 的整合，使得用户无需配置即可在 eKuiper 中对 Neuron 采集到的数据进行计算；显著降低了边缘计算解决方案对资源的使用要求，降低使用门槛。
- **[KubeEdge](./kubeedge/overview.md)**：KubeEdge 帮助实现 eKuiper 实例的容器化部署，eKuiper 可以从 MQTT 订阅设备数据，并为 KubeEdge 组件提供多功能的分析能力，以实现边缘的低延迟计算。
- **[Integration with OpenYurt](deploy/openyurt_tutorial.md)**：本节将展示如何在 OpenYurt 集群中部署 eKuiper ，并利用 yurt 隧道实现从云到边缘的管理。
