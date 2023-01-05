## Redis 源

<span style="background:green;color:white">lookup table source</span>

eKuiper 提供了对 redis 中数据查询的内置支持。请注意，现在 redis 源只能作为一个查询表使用。不支持流和扫描表。

```text
create table table1 () WITH (DATASOURCE="0", FORMAT="json", TYPE="redis", KIND="lookup");
```

您可以使用 [cli](../../../api/cli/tables.md) 或 [rest api](../../../api/restapi/tables.md) 来管理表。

Redis 源的配置文件是 */etc/sources/redis.yaml* ，可以在其中指定 redis 的连接信息等属性。

```yaml
default:
  # the redis host address
  addr: "127.0.0.1:6379"
  # currently supports string and list only
  datatype: "string"
#  username: ""
#  password: ""
```

在这个 yaml 文件的配置中，表将引用的 redis 实例地址是127.0.0.1:6379。值的类型是 "string"。