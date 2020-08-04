# Kuiper 词汇元素

## 注释

注释被当作文档。 注释以字符序列 `/*` 开始，并终止于对应的 `*/`。

## 令牌（Tokens）

Kuiper SQL 由令牌构建。 令牌分为四类：

- *标识符*
- *关键字*
- *运算符*
- *字面量*

由空格、水平制表符、回车符和换行符组成的*空白符*将被忽略，除非它将分隔的令牌分开，否则这些令牌会合并为一个令牌。在将输入分成令牌时，下一个令牌是形成有效令牌的最长字符序列。

##  标识符（Identifiers）

程序中的标识符命名实体。*标识符*是一个或多个字母和数字的序列。 标识符必须以字母开头。

如果要将保留字用作列名和流名等，它们必须用 反撇号将其括起来。 您还可以在反引号中使用各种 unicode 字符串作为 SQL 元素。 例如，运算符 `-`，空格和各种语言字符（例如中文）。

```sql
SELECT `select`, `and` from demo
SELECT `a-b`, `hello world`, `中文Chinese` from demo
```

## 关键字（Keywords）

**规则 SQL 的保留关键字**：如果您想在规则 SQL 中使用以下关键字，则必须使用反撇号将其括起来。

```
SELECT, FROM, JOIN, LEFT, INNER, ON, WHERE, GROUP, ORDER, HAVING, BY, ASC, DESC, AND, OR
```

以下是使用名为 `from` 的流的示例，`from` 是 Kuiper 中的保留关键字。

```sql
SELECT * FROM demo1 where `from`="device1"
```

**用于流管理的保留关键字**：如果您想在流管理命令中使用以下关键字，则必须使用反撇号将其括起来。

```
CREATE, RROP, EXPLAIN, DESCRIBE, SHOW, STREAM, STREAMS, WITH, BIGINT, FLOAT, STRING, DATETIME, BOOLEAN, ARRAY, STRUCT, DATASOURCE, KEY, FORMAT,CONF_KEY, TYPE, STRICT_VALIDATION, TIMESTAMP, TIMESTAMP_FORMAT
```

以下是如何在流创建语句中使用保留关键字的示例。

```sql
CREATE STREAM `stream` (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME  STRING,
					NICKNAMES  ARRAY(STRING),
					Gender     BOOLEAN,
					`地址`      STRUCT(STREET_NAME STRING, NUMBER BIGINT),
				) WITH (DATASOURCE="users", FORMAT="JSON");
```

## 运算法（Operators）

提供了以下运算符。

```
+, -, *, /, %, &, |, ^, =, !=, <, <=, >, >=, [], ->, ()
```

## 字面量（Literals）

**布尔字面量**

```
TRUE, FALSE
```

例如， ` SELECT TRUE AS field1 FROM demo` ,  `field1`字段 总是返回 `true`.

**时间字面量**： 下面的字面量在时间窗口中使用，用于标识窗口的时间单位。

```
DD, HH, MI, SS, MS
```

