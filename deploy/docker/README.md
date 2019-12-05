# `Dockerfile` links

- [emqx/kuiper](https://github.com/emqx/kuiper/blob/master/docker/Dockerfile)

# Quick reference

- **Where to get help**:

  https://emqx.io, or https://github.com/emqx/kuiper

- **Where to file issues:**

  https://github.com/emqx/kuiper/issues

- **Supported architectures**

  `amd64`, `arm64v8`,  `arm32v7`, `i386`, `ppc64le`

- **Supported Docker versions**:

  [the latest release](https://github.com/docker/docker-ce/releases/latest)

# Image Variants

The `emqx/kuiper` images come in many flavors, each designed for a specific use case.

## `emqx/kuiper:<tag>`

This is a stable release image that you can use with confidence.

## `emqx/kuiper:<tag>-<number>-<commit>`

This is an unstable version. It is an image built according to the commit number. You can use it to experience the latest features.


# What is Kuiper

A SQL based lightweight IoT analytics/streaming software running at resource constrained edge devices.

- Native run with small overhead ( ~7MB package), support Linux/Windows/Mac OS
- SQL based, easy to use
- Built-in support for MQTT source
- Extension - user can customize the rule engine
- RESTful APIs for rules management

# How to use this image

### Run kuiper

Execute some command under this docker image

```
docker run -d -v `pwd`:$somewhere emqx/kuiper:$tag $somecommand
```

For example

```
docker run -d --name kuiper -e MQTT_BROKER_ADDRESS=$MQTT_BROKER_ADDRESS emqx/kuiper:latest
```

### Configuration

Use the environment variable to configure `etc/sources/mqtt.yaml`  on the kuiper container.

| Options                    | Default            | Mapped                    |
| ---------------------------| ------------------ | ------------------------- |
| MQTT_BROKER_ADDRESS         | 127.0.0.1:1883 | default.servers |
| MQTT_BROKER_SHARED_SUBSCRIPTION | true   | default.sharedSubscription |
| MQTT_BROKER_QOS | 1                 | default.qos    |
| MQTT_BROKER_USERNAME |   | default.username |
| MQTT_BROKER_PASSWORD |                | default.password |
| MQTT_BROKER_CER_PATH |                | default.certificationPath |
| MQTT_BROKER_KEY_PATH |     | default.privateKeyPath |

If you want to configure more options, you can mount the configuration file into the kuiper container

# More

If you'd like to know more about the project, please refer to [doc home](https://github.com/emqx/kuiper/blob/master/docs/en_US/index.md).

