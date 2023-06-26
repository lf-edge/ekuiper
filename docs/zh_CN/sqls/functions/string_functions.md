# 字符串函数

字符串函数用于操作字符串数据。

## CONCAT

```text
concat(col1...)
```

连接数组或字符串。 此函数接受任意数量的参数并返回 String 或 Array。

## ENDSWITH

```text
endswith(col1, col2)
```

返回一个布尔值，该布尔值指示第一个 String 参数是否以第二个 String 参数结尾。

## FORMAT_TIME

```text
format_time(col, format)
```

将日期时间格式化为字符串。其中，若参数 col
为兼容类型，则在格式化之前[转换为 datetime 类型](./transform_functions.md#转换为-datetime-类型)
。关于格式字符串，请参考 [时间格式](#时间格式)。

### 时间格式

时间格式为一些特定符号和字母组成的字符串。eKuiper 里支持的符号如下表所示：

| 符号 | 含义       | 示例                                    |
|----|----------|---------------------------------------|
| G  | 公元       | G(AD)                                 |
| Y  | 年        | YYYY(2004), YY(04)                    |
| M  | 月        | M(1), MM(01), MMM(Jan), MMMM(January) |
| d  | 日期       | d(2), dd(02)                          |
| E  | 星期几      | EEE(Mon), EEEE(Monday)                |
| H  | 24小时制的小时 | HH(15)                                |
| h  | 12小时制的小时 | h(2), hh(03)                          |
| a  | AM 或 PM  | a(PM)                                 |
| m  | 分        | m(4), mm(04)                          |
| s  | 秒        | s(5), ss(05)                          |
| S  | 秒的分数     | S(.0), SS(.00), SSS(.000)             |
| z  | 时区名      | z(MST)                                |
| Z  | 4位数的时区   | Z(-0700)                              |
| X  | 时区       | X(-07), XX(-0700), XXX(-07:00)        |

示例:

- YYYY-MM-dd T HH:mm:ss -> 2006-01-02 T 15:04:05
- YYYY/MM/dd HH:mm:ssSSS XXX -> 2006/01/02 15:04:05.000 -07:00

## INDEXOF

```text
indexof(col1, col2)
```

返回第二个参数的第一个索引（从0开始），作为第一个参数中的子字符串。

## LENGTH

```text
length(col)
```

返回提供的字符串中的字符数。

## LOWER

```text
lower(col)
```

返回给定 String 的小写版本。

## LPAD

```text
lpad(col, padNum)
```

返回 String，在左侧用第二个参数指定的空格数填充。

## LTRIM

```text
ltrim(col)
```

从提供的字符串中删除所有前导空格（制表符和空格）。

## NUMBYTES

```text
numbytes(col)
```

返回提供的字符串中的字节数。

## REGEXP_MATCHES

```text
regexp_matches(col, regex)
```

如果字符串（第一个参数）包含正则表达式的匹配项，则返回 true。

## REGEXP_REPLACE

```text
regexp_replace(col, regex, str)
```

将第一个参数中所有出现的第二个参数（正则表达式）替换为第三个参数。

## REGEXP_SUBSTR

```text
regexp_substr(col, regex)
```

在第一个参数中找到第二个参数（regex）的第一个匹配项。

## RPAD

```text
rpad(col, padNum)
```

返回 String，在右侧填充第二个参数指定的空格数。

## RTRIM

```text
rtrim(col)
```

从提供的字符串中删除所有尾随空白（制表符和空格）。

## SUBSTRING

```text
substring(col, start, end)
```

返回 String，其中包含从 start 到 end 的子字符串。如果 end 为负数，则从字符串末尾开始计数。

## STARTSWITH

```text
startswith(col, str)
```

返回布尔值，是否第一个字符串参数是否以第二个字符串参数开头。

## SPLIT_VALUE

```text
split_value(col, splitter, index)
```

将第一个字符串参数以第二个字符串参数作为分隔符切分，返回切分后的第 index（参数三）个值。

## TRIM

```text
trim(col)
```

从提供的字符串中删除所有前导和尾随空格（制表符和空格）。

## UPPER

```text
upper(col)
```

返回给定 String 的大写版本。
