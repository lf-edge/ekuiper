# Nop action

The action is an Nop sink, the result sent to this sink will be ignored. If specify the `log` property to `true`, then the result will be saved into log file, the log file is at `$eKuiper_install/log/stream.log` by default.

| Property name      | Optional | Description                                                  |
| ------------------ | -------- | ------------------------------------------------------------ |
| log             | true | true/false - print the sink result to log or not. By default is `false`, that will not print the result to log file. |


