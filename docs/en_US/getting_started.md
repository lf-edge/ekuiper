

## Download & install

Get the installation package via https://github.com/lf-edge/ekuiper/releases or https://www.emqx.io/downloads#kuiper

### zip、tar.gz compressed package

Unzip eKuiper

```sh
$ unzip kuiper-$VERISON-$OS-$ARCH.zip
or
$ tar -xzf kuiper-$VERISON-$OS-$ARCH.zip
```

Run `bin/kuiperd` to start the eKuiper server

```sh
$ bin/kuiperd
```

You should see a successful message: `Serving Rule server on port 20498`

The directory structure of eKuiper is as follows:

```
eKuiper_installed_dir
  bin
    server
    cli
  etc
    mqtt_source.yaml
    ...
  data
    ...
  plugins
    ...
  log
    ...
```


#### deb、rpm installation package

Use related commands to install eKuiper

```sh
$ sudo dpkg -i kuiper_$VERSION_$ARCH.deb
or
$ sudo rpm -ivh kuiper-$VERSION-1.el7.rpm
```

Run `kuiperd` to start the eKuiper server

```sh
$ sudo kuiperd
```

You should see a successful message: `Serving Rule server on port 20498`

eKuiper also supports systemctl startup

 ```sh
 $ sudo systemctl start kuiper
 ```

The directory structure of eKuiper is as follows:

```
/usr/lib/kuiper/bin
  server
  cli
/etc/kuiper
  mqtt_source.yaml
  ...
/var/lib/kuiper/data
  ...
/var/lib/kuiper/plugins
  ...
/var/log/kuiper
   ...
```



## Run the first rule stream

eKuiper rule is composed by a SQL and multiple actions. eKuiper SQL is an easy to use SQL-like language to specify the logic of the rule stream. By providing the rule through CLI, a rule stream will be created in the rule engine and run continuously. The user can then manage the rules through CLI.

eKuiper has a lot of built-in functions and extensions available for complex analysis, and you can find more information about the grammer and its functions from the [eKuiper SQL reference](sqls/overview.md).

Let's consider a sample scenario where we are receiving temperature and humidity record from a sensor through MQTT service and we want to issue an alert when the temperature is bigger than 30 degrees celcius in a time window. We can write a eKuiper rule for the above scenario using the following several steps.

### Prerequisite

We assume there is already a MQTT broker as the data source of eKuiper server. If you don't have one, EMQX is recommended. Please follow the [EMQ Installation Guide](https://docs.emqx.io/en/broker/latest/getting-started/install.html) to setup a mqtt broker.

### Defining the input stream

The stream needs to have a name and a schema defining the data that each incoming event should contain. For this scenario, we will use an MQTT source to consume temperature events. The input stream can be defined by SQL language.

We create a stream named `demo` which consumes MQTT `demo` topic as specified in the DATASOURCE property.
```sh
$ bin/kuiper create stream demo '(temperature float, humidity bigint) WITH (FORMAT="JSON", DATASOURCE="demo")'
```
The MQTT source will connect to MQTT broker at `tcp://localhost:1883`. If your MQTT broker is in another location, specify it in the `etc/mqtt_source.yaml`.  You can change the servers configuration as in below.

```yaml
default:
  qos: 1
  sharedsubscription: true
  servers: [tcp://127.0.0.1:1883]
```

You can use command `kuiper show streams` to see if the `demo` stream was created or not.

### Testing the stream through query tool

Now the stream is created, it can be tested from `kuiper query` command. The `kuiper` prompt is displayed as below after typing `cli query`.

```sh
$ bin/kuiper query
kuiper > 
```

In the `kuiper` prompt, you can type SQL and validate the SQL against the stream.

```sh
kuiper > select count(*), avg(humidity) as avg_hum, max(humidity) as max_hum from demo where temperature > 30 group by TUMBLINGWINDOW(ss, 5);

query is submit successfully.
```

Now if any data are published to the MQTT server available at `tcp://127.0.0.1:1883`, then it prints message as following.

```
kuiper > [{"avg_hum":41,"count":4,"max_hum":91}]
[{"avg_hum":62,"count":5,"max_hum":96}]
[{"avg_hum":36,"count":3,"max_hum":63}]
[{"avg_hum":48,"count":3,"max_hum":71}]
[{"avg_hum":40,"count":3,"max_hum":69}]
[{"avg_hum":44,"count":4,"max_hum":57}]
[{"avg_hum":42,"count":3,"max_hum":74}]
[{"avg_hum":53,"count":3,"max_hum":81}]
...
```

You can press `ctrl + c` to break the query, and server will terminate streaming if detecting client disconnects from the query. Below is the log print at server.

```
...
time="2019-09-09T21:46:54+08:00" level=info msg="The client seems no longer fetch the query result, stop the query now."
time="2019-09-09T21:46:54+08:00" level=info msg="stop the query."
...
```

### Writing the rule

As part of the rule, we need to specify the following:
* rule name: the id of the rule. It must be unique
* sql: the query to run for the rule
* actions: the output actions for the rule

We can run the `kuiper rule` command to create rule and specify the rule definition in a file

```sh
$ bin/kuiper create rule ruleDemo -f myRule
```
The content of `myRule` file. It prints out to the log  for the events where the average temperature in a 1 minute tumbling window is bigger than 30.
```json
{
    "sql": "SELECT temperature from demo where temperature > 30",
    "actions": [{
        "log":  {}
    }]
}
```
You should see a successful message `rule ruleDemo created` in the stream log, and the rule is now set up and running.

### Testing the rule
Now the rule engine is ready to receive events from  MQTT `demo`  topic. To test it, just use a MQTT client to publish message to the `demo` topic. The message should be in json format like this:
```json
{"temperature":31.2, "humidity": 77}
```

Check the stream log located at "`log/stream.log`", and you would see the filtered data are printed out. Also, if you send below message, it does not meet the SQL condition, and the message will be filtered.

```json
{"temperature":29, "humidity": 80}
```

### Managing the rules
You can use command line tool to stop the rule for a while and restart it and other management work. The rule name is the identifier of a rule. Check [Rule Management CLI](operation/cli/rules.md) for detail
```sh
$ bin/kuiper stop rule ruleDemo
```

Refer to the following topics for guidance on using the eKuiper.

- [Command line interface tools - CLI](operation/cli/overview.md)
- [eKuiper SQL reference](./sqls/overview.md)
- [Rules](./rules/overview.md)
- [Extend eKuiper](./extension/overview.md)
- [Plugins](extension/native/develop/overview.md)
