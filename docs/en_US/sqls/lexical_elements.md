# eKuiper lexical elements

## Comments

Comment serve as documentation. Comments begin with the character sequence `/*` and stop at the pair `*/`.

## Tokens

eKuiper SQL is built up from tokens. There are four classes of tokens:

- *identifiers*
- *keywords*
- *operators*
- *literals*

*White space* formed from spaces, horizontal tabs, carriage returns, and newlines is ignored except as it separates tokens that would otherwise combine into a single token. While breaking the input into tokens, the next token is the longest sequence of characters that form a valid token.

## Identifiers

Identifiers name entities within a program. An *identifier* is a sequence of one or more letters and digits. An identifier must start with a letter. 

To use the reserved words as the column name and the stream name etc, they need to be quoted by backtick. You can also use all kinds of unicode string in the backtick as an SQL element. For example, operator `-`, spaces, and various language characters such as Chinese.

```sql
SELECT `select`, `and` from demo
SELECT `a-b`, `hello world`, `中文Chinese` from demo
```

## Keywords

**Reserved keywords for rule SQL**: If you'd like to use the following keyword in rule SQL, you will have to use backtick to enclose them.

```
SELECT, FROM, JOIN, LEFT, INNER, ON, WHERE, GROUP, ORDER, HAVING, BY, ASC, DESC, AND, OR, CASE, WHEN, THEN, ELSE, END
```

The following is an example for using a stream named `from`, which is a reserved keyword in eKuiper.

```sql
SELECT * FROM demo1 where `from`="device1"
```

**Reserved keywords for streams management**: If you'd like to use the following keywords in stream management command, you will have to use backtick to enclose them.

```
CREATE, RROP, EXPLAIN, DESCRIBE, SHOW, STREAM, STREAMS, WITH, BIGINT, FLOAT, STRING, DATETIME, BOOLEAN, ARRAY, STRUCT, DATASOURCE, KEY, FORMAT,CONF_KEY, TYPE, STRICT_VALIDATION, TIMESTAMP, TIMESTAMP_FORMAT
```

The following is an example for how to use reserved keywords in stream creation statement.

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

## Operators

Following operators are provided.

```
+, -, *, /, %, &, |, ^, =, !=, <, <=, >, >=, [], ->, (), IN, NOT IN
```

## Literals

**Boolean literals**

```
TRUE, FALSE
```

Example, ` SELECT TRUE AS field1 FROM demo` , the field `field1` always returns `true`.

**Time literals**: Below literals are used in time window, which identify the time unit for windows.

```
DD, HH, MI, SS, MS
```

