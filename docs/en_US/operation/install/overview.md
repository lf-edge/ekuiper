## Installation instruction

Please download the installation package, and refer to below for the instruction of installing for different operate systems.

- [Cent-OS](./cent-os.md)
- ...

## Installation structure 

Below is the directory structure after installation. 

```shell
bin
  cli
etc
  mqtt_source.yaml
  *.yaml
data
plugins
log
```

### bin

The ``bin`` directory includes all of executable files. Such as ``kuiper`` command.

### etc

The ``etc`` directory contains the configuration files of eKuiper. Such as MQTT source configurations etc.

### data

eKuiper persistences all the definitions of streams and rules, and all of message will be stored in this folder  for long duration operations.

### plugins

eKuiper allows users to develop your own plugins, and put these plugins into this folder.  See [extension](../../extension/overview.md) for more info for how to extend the eKuiper.

### log

All of the log files are under this folder. The default log file name is ``stream.log``.

## Next steps

- See [getting started](../../getting_started.md) for your first eKuiper experience.
- See [CLI tools](../cli/overview.md) for usage of eKuiper CLI tools.

