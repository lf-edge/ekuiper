# Extensions

Kuiper allows user to customize the different kinds of extensions.  

- The source extension is used for extending different stream source, such as consuming data from other message brokers. Kuiper has built-in source support for [MQTT broker](../rules/sources/mqtt.md).
- Sink/Action extension is used for extending pub/push data to different targets, such as database, other message system, web interfaces or file systems. Built-in action support in Kuiper, see [MQTT](../rules/sinks/mqtt.md) & [log files](../rules/sinks/logs.md).
- Functions extension allows user to extend different functions that used in SQL. Built-in functions supported in Kuiper, see [functions](../sqls/built-in_functions.md).

Please read below for how to realize the different extensions.

- [Source extension](#)
- [Sink/Action extension](#)
- [Functions extension](#)

