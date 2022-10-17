# Ruleset Management

The eKuiper rule command line tools allows to import and export all the stream and rule configurations.

## Ruleset Format

The file format for importing and exporting ruleset is JSON, which can contain three parts: `streams`, `tables` and `rules`. Each type holds the the key-value pair of the name and the creation statement. In the following example file, we define a stream and two rules.

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

## Import Ruleset

This command accepts the ruleset and imports it into the system. If a stream or rule in the ruleset already exists, it is not created. The imported rules are started immediately. The command returns text about the number of streams and rules created


```shell
# bin/kuiper import ruleset -f myrules.json
```

## Export Ruleset

This command exports the ruleset to the specified file. The command returns text about the number of streams and rules exported.

```shell
# bin/kuiper export ruleset myrules.json
```