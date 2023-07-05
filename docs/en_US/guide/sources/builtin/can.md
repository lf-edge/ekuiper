# CAN Source

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

CAN source is provided to consume events from a socket CAN interface for a CAN bus.

## Configurations

Update the configuration at `data/sources/can.yaml`. Below is the default configuration.

```yaml
default:
  # The network type of the CAN bus, can be can or udp
  network: can
  # The address of the CAN bus
  address: can0
```

There are two properties to configure:

### network

The network type of the CAN bus, can be `can` or `udp`.

### address

The address of the CAN bus.

For `can` network, it is the name of the CAN interface. Use `ip link show` to list all the interfaces in your system and
find out the CAN interfaces.
For `udp` network, it is the address of the UDP server.
