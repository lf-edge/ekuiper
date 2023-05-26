# String Functions

String functions are used to manipulate string data.

## CONCAT

```
concat(col1, col2, ...)
```

Concatenates arrays or strings. This function accepts any number of arguments and returns a string or an array.

## ENDSWITH

```
endswith(col1, col2)
```

Returns a boolean indicating whether the first string argument ends with the second string argument.

## FORMAT_TIME

```
format_time(col, format)
```

Format a datetime to string. The 'col' will be [cast to datetime type](./transform_functions.md#cast-to-datetime) if it
is
bigint, float or string type before formatting. Please check [format patterns](#formattime-patterns) for how to compose
the format.

### Format_time patterns

A pattern is used to create a format string. Patterns are based on a simple sequence of letters and symbols which is
common in many languages like Java, etc. The supported symbols in Kuiper are

| Symbol | Meaning                   | Example                               |
|--------|---------------------------|---------------------------------------|
| G      | era                       | G(AD)                                 |
| Y      | year                      | YYYY(2004), YY(04)                    |
| M      | month                     | M(1), MM(01), MMM(Jan), MMMM(January) |
| d      | day of month              | d(2), dd(02)                          |
| E      | day of week               | EEE(Mon), EEEE(Monday)                |
| H      | hour in 24 hours format   | HH(15)                                |
| h      | hour in 12 hours format   | h(2), hh(03)                          |
| a      | AM or PM                  | a(PM)                                 |
| m      | minute                    | m(4), mm(04)                          |
| s      | second                    | s(5), ss(05)                          |
| S      | fraction of second        | S(.0), SS(.00), SSS(.000)             |
| z      | time zone name            | z(MST)                                |
| Z      | 4 digits time zone offset | Z(-0700)                              |
| X      | time zone offset          | X(-07), XX(-0700), XXX(-07:00)        |

Examples:

- YYYY-MM-dd T HH:mm:ss -> 2006-01-02 T 15:04:05
- YYYY/MM/dd HH:mm:ssSSS XXX -> 2006/01/02 15:04:05.000 -07:00

## INDEXOF

```
indexof(col1, col2)
```

Returns the first index (0-based) of the second argument as a substring in the first argument.

## LENGTH

```
length(col)
```

Returns the number of characters in the provided string.

## LOWER

```
lower(col)
```

Returns the lowercase version of the given string.

## LPAD

```
lpad(col, 2)
```

Returns the string argument, padded on the left side with the number of spaces specified by the second argument.

## LTRIM

```
ltrim(col)
```

Removes all leading whitespace (tabs and spaces) from the provided string.

## NUMBYTES

```
numbytes(col)
```

Returns the number of bytes in the UTF-8 encoding of the provided string.

## REGEXP_MATCHES

```
regexp_matches(col, regex)
```

Returns true if the string (first argument) contains a match for the regular expression.

## REGEXP_REPLACE

```
regexp_replace(col, regex, replacement)
```

Replaces all substrings of the specified string value that matches regexp with replacement.

## REGEXP_SUBSTRING

```
regexp_substring(col, regex)
```

Returns the first substring of the specified string value that matches regexp.

## RPAD

```
rpad(col, 2)
```

Returns the string argument, padded on the right side with the number of spaces specified by the second argument.

## RTRIM

```
rtrim(col)
```

Removes all trailing whitespace (tabs and spaces) from the provided string.

## SUBSTRING

```
substring(col, start, length)
```

Returns the substring of the specified string value starting at the specified index position (0-based, inclusive) for up
to the specified length of characters.

## STARTSWITH

```
startswith(col, str)
```

Returns a boolean indicating whether the first string argument starts with the second string argument.

## SPLIT_VALUE

```
split_value(col, str_splitter, index)
```

Split the value of the 1st parameter with the 2nd parameter, and return the value of split array that indexed with the
3rd parameter.

For example, `split_value("/test/device001/message","/",0) AS a`, the returned value of function is empty;

`split_value("/test/device001/message","/",3) AS a`, the returned value of function is `message`.

## TRIM

```
trim(col)
```

Removes all leading and trailing whitespace (tabs and spaces) from the provided string.

## UPPER

```
upper(col)
```

Returns the uppercase version of the given string.