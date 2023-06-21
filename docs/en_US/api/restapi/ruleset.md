# Ruleset Management

eKuiper REST api allows to import or export the stream and rule configurations.

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

The API accepts rulesets and imports them into the system. If a stream or rule in the ruleset already exists, it is not created. The API returns text informing the number of streams and rules created. The API supports specifying rulesets by means of text content or file URIs.

Example 1: Import by text content

```shell
POST http://{{host}}/ruleset/import
Content-Type: application/json

{
  "content": "{json of the ruleset}"
}
```

Example 2: Import by file URI

```shell
POST http://{{host}}/ruleset/import
Content-Type: application/json

{
  "file": "file:///tmp/a.json"
}
```

## Export Ruleset

The export API returns a file to download.

```shell
POST http://{{host}}/ruleset/export
```
