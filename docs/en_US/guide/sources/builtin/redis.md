## Redis source

<span style="background:green;color:white">lookup table source</span>

eKuiper provides built-in support for looking up data in redis. Notice that, the redis source can only be used as a lookup table now. Stream and scan table is not supported.

```text
create table table1 () WITH (DATASOURCE="0", FORMAT="json", TYPE="redis", KIND="lookup");
```

You can use [cli](../../../api/cli/tables.md) or [rest api](../../../api/restapi/tables.md) to manage the tables.

The configure file for the redis source is in */etc/sources/redis.yaml* in which the path to the file can be specified.

```yaml
default:
  # the redis host address
  addr: "127.0.0.1:6379"
  # currently supports string and list only
  datatype: "string"
#  username: ""
#  password: ""
```

With this yaml file, the table will refer to the database 0 in redis instance of address 127.0.0.1:6379. The value type is `string`.
