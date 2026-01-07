# eKuiper lexical elements

## Comments

Comment serve as documentation. Comments begin with the character sequence `/*` and stop at the pair `*/`.

## Tokens

eKuiper SQL is built up from tokens. There are four classes of tokens:

- _identifiers_
- _keywords_
- _operators_
- _literals_

_White space_ formed from spaces, horizontal tabs, carriage returns, and newlines is ignored except as it separates tokens that would otherwise combine into a single token. While breaking the input into tokens, the next token is the longest sequence of characters that form a valid token.

## Identifiers

Identifiers name entities within a program. An _identifier_ is a sequence of one or more letters and digits. An identifier must start with a letter.

To use the reserved words as the column name and the stream name etc, they need to be quoted by backtick. You can also use all kinds of unicode string in the backtick as an SQL element. For example, operator `-`, spaces, and various language characters such as Chinese.

```sql
SELECT `select`, `and` from demo
SELECT `a-b`, `hello world`, `中文Chinese` from demo
```

## Keywords

**Reserved keywords for rule SQL**: If you'd like to use the following keyword in rule SQL, you will have to use backtick to enclose them.

```text
SELECT, FROM, JOIN, LEFT, INNER, ON, WHERE, GROUP, ORDER, HAVING, BY, ASC, DESC, AND, OR, CASE, WHEN, THEN, ELSE, END, IN, NOT, BETWEEN, LIKE, OVER, PARTITION
```

The following is an example for using a stream named `from`, which is a reserved keyword in eKuiper.

```sql
SELECT * FROM demo1 where `from`="device1"
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

```text
+, -, *, /, %, &, |, ^, =, !=, <, <=, >, >=, [], ->, (), IN, NOT IN, BETWEEN, NOT BETWEEN
```

## Literals

**Boolean literals**

```text
TRUE, FALSE
```

Example, `SELECT TRUE AS field1 FROM demo` , the field `field1` always returns `true`.

**Time literals**: Below literals are used in time window, which identify the time unit for windows.

```text
DD, HH, MI, SS, MS
```

**String Literals**:

```text
"user", 'user'
```

Please note that when using single quotes for string literals in the command line, you may encounter the following issue:

```text
$ bin/kuiper create rule myrule '{"sql": "SELECT lower('abc') FROM demo"...}'
```

When creating a rule using the above command, the string literal 'abc' within the single quotes will be interpreted as the variable abc. This is due to the referencing mechanism in the Shell:

```text
$ echo '{"sql": "SELECT lower('abc') FROM demo"}'
{"sql": "SELECT lower(abc) FROM demo"}
```

If you encounter this issue, it is recommended to use double quotes for string literals "abc" instead of single quotes 'abc' to prevent variable substitution from occurring.
