# Nop action

该 action 是一个空操作目标，所有发送到此的结果将被忽略。如果指定 ``log`` 属性为 ``true``，那么结果将会保存到日志文件，日志文件缺省保存在  `` $kuiper_install/log/stream.log``。

| Property name      | Optional | Description                                                  |
| ------------------ | -------- | ------------------------------------------------------------ |
| log             | true | true/false - 是否将结果打印到日志。缺省为 ``false``，这种情况下将不会打印到日志文件。 |


