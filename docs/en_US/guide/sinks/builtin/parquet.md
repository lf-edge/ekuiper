# Parquet Sink

The sink saves the analysis result to [Apache Parquet](https://parquet.apache.org/) files. The columns and their types
are declared in the rule.

::: tip
This sink is only available when eKuiper is built with the `parquet` or the `full` build tag, for example
`go build -tags parquet ./cmd/kuiperd`.
:::

## Properties

The only new property is `columns`, which is required. The `path` property and the
[rolling properties](./file.md#rolling-strategy) work as in the [file sink](./file.md), with these differences:

- `path` does not support templates.
- `rollingSize`, `compression`, `encryption` and `rollingHook` are not supported. A rule using them fails to be created.
- Without a `rollingNamePattern`, or with `none`, every roll and every rule restart replaces the previous file. Use
  `prefix` or `suffix` to keep all the files.
- Leave the `format` property unset. It only defines how to parse the output of a `dataTemplate`.

Rows are buffered in memory and may only reach the disk when the file is closed. If the process crashes, the rows of the
open file are lost.

### Columns

The `columns` property declares each column and its type:

```json
{
  "columns": {
    "id": "int64",
    "temperature": "double",
    "location": "string?",
    "active": "boolean"
  }
}
```

The supported types are `boolean`, `int64`, `double` and `string`, in upper or lower case. A column is required by
default. Add a `?` to the type to make it nullable.

The columns are always stored in alphabetical order, regardless of the order in the configuration.

### Type check

Values are never converted. Each value must already have the exact type of its column, otherwise the row is rejected.
A rejected row is reported as a rule error and is not written, and the rule keeps running. If a row of a batch is
rejected, the whole batch is not written.

| Row                                    | Result                                |
|----------------------------------------|---------------------------------------|
| The value has a different type         | The row is rejected                   |
| The field is not declared in `columns` | The row is rejected                   |
| `null` in a required column            | The row is rejected                   |
| `null` in a nullable column            | Written as null                       |
| A nullable column is absent            | Written as null                       |
| A required column is absent            | Written as the zero value of the type |

The zero values are `0` for `int64` and `double`, an empty string for `string` and `false` for `boolean`. They cannot be
told apart from real values, so declare a column as nullable if a value may be absent.

Every field in the rule output must be declared. For example, `SELECT *` only works if `columns` lists all the fields.
To save only some fields, select them in the SQL statement or use the `fields` common property.

::: warning
Declare the schema of the stream when you use this sink. Without a schema, JSON numbers are decoded as `double`. An
`int64` column then rejects every row, even for a value like `21`. If you set `useInt64ForWholeNumber` instead, a `double`
column rejects whole numbers like `21`. The output of a `dataTemplate` is decoded in the same way, so a `dataTemplate`
cannot fill an `int64` column.
:::

## Sample usage

Below is a sample that saves the readings with a temperature greater than 50 degrees. The stream declares its schema, so
that the values have the types of the columns:

```sql
CREATE STREAM demo (id BIGINT, temperature FLOAT, location STRING) WITH (DATASOURCE="demo", FORMAT="json")
```

A new file is created every hour or every 10000 rows, with a name like `result-1699888888.parquet`.

```json
{
  "sql": "SELECT id, temperature, location FROM demo WHERE temperature > 50",
  "actions": [
    {
      "parquet": {
        "path": "result.parquet",
        "columns": {
          "id": "int64",
          "temperature": "double",
          "location": "string?"
        },
        "rollingInterval": 3600000,
        "checkInterval": 600000,
        "rollingCount": 10000,
        "rollingNamePattern": "suffix"
      }
    }
  ]
}
```
