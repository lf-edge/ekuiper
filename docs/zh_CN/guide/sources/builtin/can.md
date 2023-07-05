# CAN 源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

CAN 源用于消费来自 CAN 总线的 SocketCAN 接口的事件。

## 配置

可更新配置文件 `data/etc/sources/can.yaml` 。 以下为默认的配置。

```yaml
default:
  # The network type of the CAN bus, can be can or udp
  network: can
  # The address of the CAN bus
  address: can0
```

有两个配置项可供配置:

### network

CAN总线的网络类型，可以是 `can` 或 `udp` 。

### address

CAN总线的地址。

对于 `can` 网络，它是 CAN 接口的名称。使用 `ip link show` 来列出系统中的所有接口，并找出 CAN 接口。
对于 `udp` 网络，它是UDP服务器的地址。
