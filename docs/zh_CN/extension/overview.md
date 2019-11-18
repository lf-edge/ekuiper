# 扩展

Kuiper允许用户自定义不同类型的扩展。 

- 源扩展用于扩展不同的流源，例如使用来自其他消息服务器的数据。 Kuiper对 [MQTT消息服务器](../rules/sources/mqtt.md)的内置源提供支持。
- Sink / Action扩展用于将发布/推送数据扩展到不同的目标，例如数据库，其他消息系统，Web界面或文件系统。 Kuiper中提供内置动作支持，请参阅 [MQTT](../rules/sinks/mqtt.md) & [日志文件](../rules/sinks/logs.md).。
- 功能扩展允许用户扩展SQL中使用的不同功能。 Kuiper支持内置功能，请参见 [functions](../sqls/built-in_functions.md)。

请阅读以下内容，了解如何实现不同的扩展。

- [源扩展](#)
- [Sink/Action 扩展](#)
- [功能扩展](#)

