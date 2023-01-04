# 规则集管理

eKuiper 命令行工具允许您导入导出当前的所有流和规则配置。

## 规则集格式

导入导出规则集的文件格式为 JSON，其中可包含三个部分：流 `streams`，表 `tables` 和规则 `rules`。每种类型保存名字和创建语句的键值对。在以下示例文件中，我们定义了一个流和两条规则。

```json
{
    "streams": {
        "demo": "CREATE STREAM demo () WITH (DATASOURCE=\"users\", FORMAT=\"JSON\")"
    },
    "tables": {},
    "rules": {
        "rule1": "{\"id\": \"rule1\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{\"log\": {}}]}",
        "rule2": "{\"id\": \"rule2\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{  \"log\": {}}]}"
    }
}
```

## 导入规则集

该指令接受规则集并将其导入系统中。若规则集中的流或规则已存在，则不再创建。导入的规则将立刻启动。指令返回文本告知创建的流和规则的数目。


```shell
# bin/kuiper import ruleset -f myrules.json
```

## 导出规则集

该指令导出规则集到指定的文件中。指令返回文本告知导出的流和规则的数目。

```shell
# bin/kuiper export ruleset myrules.json
```