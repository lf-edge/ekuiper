# Stream Processing CAN bus Data

eKuiper supports to process CAN bus data.
It can be used to process the CAN bus data directly by socketCan or to process the CAN bus data from other protocols,
such as MQTT.

In this tutorial, we will guide you to create streams and rules to process the CAN bus data by the two ways.

## Prerequisites

DBC file defines the CAN bus signals.
We use the DBC file to decode the CAN bus data into readable signals.
So, before running the demo, you need to prepare the DBC file.
We already prepare sample dbc files in the `dbc` folder.
You can replace them with your own dbc files.

## Connect by SocketCAN

SocketCAN is a networking protocol implementation in the Linux kernel
that provides a socket-based interface for communicating with Controller Area Network
(CAN) devices.
It is part of the linux kernel.

### Setup CAN interface

If you connect to the CAN bus with real hardware, you'll have a native CAN interfaces.
Use `ip link show` command to list all the interfaces, and the interface of type `link/can` is the CAN interface.

If you don't have a real CAN interface, you can use the virtual CAN interface.
Take ubuntu as an example, we can enable a virtual CAN interface by the following commands:

```bash
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan
sudo ip link set up can0
```

Check the interface by `ip link show` command, you will see the interface `vcan0` is created.

### Send/Receive CAN data

We will install `can-utils` to send/receive CAN data.

```bash
sudo apt install can-utils
```

Then we can receive and print the raw CAN data by the following commands:

```bash
candump can0
```

In another terminal, we can send CAN data for testing:

```bash
cansend can0 123#1122334455667788
```

In which, the `123` is the CAN ID, and `1122334455667788` is the data payload.
Make sure the data is printed out in the first terminal.
Until now, our CAN interface is ready.

In the next section, we will use `cansend` to send test data to eKuiper.

### Create Rules to process CAN data

Firstly, we need to create a stream to connect to the virtual can interface `can0`. The stream definition is as below:

```sql
create stream canDemo () WITH (TYPE="can", CONF_KEY="default", FORMAT="can", SHARED="true", SCHEMAID="dbc")
```

- `TYPE="can"`: The stream type is `can`, which will connect to the CAN bus by socketCan.
- `CONF_KEY="default"`: The configuration key is `default`, which will use the default configuration in the configuration file. The default configuration is in `etc/sources/can.yaml` which defines the can address to `can0`. You can override with your own configuration at `data/sources/can.yaml`.
- `FORMAT="can"`: The format of the data is `can`, which will parse the raw CAN data for each can frame into a map of signals with dbc.
- `SHARED="true"`: The stream is shared, which means the stream will be shared by all the rules.
- `SCHEMAID="dbc"`: The schema of the stream is `dbc`, which will use the dbc files inside the `dbc` folder to parse the raw CAN data.

Then we can create a rule to print the data:

```json
{
  "id": "canAll",
  "sql": "Select * From canDemo",
  "actions": [
    {
      "log": {}
    }
  ]
}
```

We create the simplest rule named `canAll` to select all the data from the stream `canDemo` and print it out.
It will show that the raw data are parsed into a map of signals.

### Test the rule

Send test data to the can interface `can0` by `cansend` command:

```bash
cansend can0 586#5465737400000000
```

Make sure the data can fit your DBC file.
The ID must be presented in the DBC file and the data payload must be the same length as defined in the DBC file.

Then check the log of the rule `canAll`, you should receive messages like:

```json
{
  "VBBrkCntlAccelPedal": 0,
  "VBTOSLatPstn":   87.125,
  "VBTOSLonPstn":   168.75,
  "VBTOSObjID":     0,
  "VBTOSTTC":       46.400000000000006
}
```

## Connect by Other Protocols with CANJSON format

Due to security or privacy reasons, we may not want to connect to the CAN bus directly.
Typically, user will have a gateway to receive the CAN data and send it to applications by other protocols such as TCP,
UDP or MQTT.
And the gateway will packet multiple CAN frames into one message.

In this section, we'll use MQTT as an example to show how to process the CAN data from other protocols.
The serialization format may be private.
And we'll use the CANJSON format which packet multiple CAN frames into a JSON to send the CAN data.

### Create Rules to process CAN data

Firstly, we need to create a stream to connect to MQTT to receive the data. The stream definition is as below:

```sql
create stream mqttCanDemo () WITH (TYPE="mqtt", CONF_KEY="default", FORMAT="canjson", SHARED="true", SCHEMAID="dbc", DATASOURCE="canDemo")
```

- `TYPE="mqtt"`: The stream type is `mqtt`, which will connect to a MQTT broker and subscribe to a topic.
- `DATASOURCE="canDemo"`: The topic to subscribe is `canDemo`. You can change it to your own topic.
- `CONF_KEY="default"`: The configuration key is `default` which defines the MQTT connection properties in `etc/mqtt_source.yaml`.
- `FORMAT="canjson"`: The format of the data is `canjson`, which will parse the json of multiple can frames into a map of signals with dbc.
- `SHARED="true"`: The stream is shared, which means the stream will be shared by all the rules.
- `SCHEMAID="dbc"`: The schema of the stream is `dbc`, which will use the dbc files inside the `dbc` folder to parse the raw CAN data.

Then we can create a rule to print the data:

```json
{
  "id": "canAll2",
  "sql": "Select * From mqttCanDemo",
  "actions": [
    {
      "log": {}
    }
  ]
}
```

We create the simplest rule named `canAll2` to select all the data from the stream `mqttCanDemo` and print it out.
It will show that the raw data are parsed into a map of signals.

### Test the rule

Send test data to the MQTT topic `canDemo`.
The frames can contain any number of CAN frames.
Make sure each can frame id are defined in the DBC file.

```json
{
   "frames": [
      {
         "id": 1006,
         "data": "54657374000000005465737400000000"
      },
      {
         "id": 1414,
         "data": "5465737400000000"
      }
   ]
}
```

We will get output similar to:

```json
{
  "VBBrkCntlAccelPedal": 0,
  "VBTOSLatPstn":   87.125,
  "VBTOSLonPstn":   168.75,
  "VBTOSObjID":     0,
  "VBTOSTTC":       46.400000000000006
}
```

## Further processing

Now that we have the CAN data in the map format,
we can do a further process on the data just like JSON data that we receive from MQTT or other protocols.
Check the doc for more scenarios.
