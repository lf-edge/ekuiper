## 文件源

<span style="background:green;color:white">scan table source</span>

eKuiper 提供了内置支持，可将文件内容读入 eKuiper 处理管道。 文件源通常用作 [表格](../../../sqls/tables.md)， 并且采用 create table 语句的默认类型。

```sql
CREATE TABLE table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="lookup.json", FORMAT="json", TYPE="file");
```

您可以使用 [cli](../../../operation/cli/tables.md) 或 [rest api](../../../operation/restapi/tables.md) 来管理表。

文件源的配置文件是 */etc/sources/file.yaml* ，可以在其中指定文件的路径。

```yaml
default:
  fileType: json
  # 文件以 eKuiper 为根目录的目录或文件的绝对路径。
  # 请勿在此处包含文件名。文件名应在流数据源中定义
  path: data
  # 读取文件的时间间隔，单位为ms。 如果只读取一次，则将其设置为 0
  interval: 0
```

通过这个 yaml 文件，该表将引用文件 *${eKuiper}/data/lookup.json* 并以 json 格式读取它。