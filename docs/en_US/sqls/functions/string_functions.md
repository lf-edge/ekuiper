# String Functions

String functions are used to manipulate string data.

## CONCAT

```text
concat(col1, col2, ...)
```

Concatenates arrays or strings. This function accepts any number of arguments and returns a string or an array.

## ENDSWITH

```text
endswith(col1, col2)
```

Returns a boolean indicating whether the first string argument ends with the second string argument.

## FORMAT_TIME

```text
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
| ------ | ------------------------- | ------------------------------------- |
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
| \      | Escape character          | \Z(Z) \X(X)                           |

Examples:

- YYYY-MM-dd T HH:mm:ss -> 2006-01-02 T 15:04:05
- YYYY/MM/dd HH:mm:ssSSS XXX -> 2006/01/02 15:04:05.000 -07:00
- yyyy-MM-ddTHH:mm:ssSS\ZXX -> 2006-01-02T15:04:05.00Z-0700

## INDEXOF

```text
indexof(col1, col2)
```

Returns the first index (0-based) of the second argument as a substring in the first argument.

## LENGTH

```text
length(col)
```

Returns the number of characters in the provided string.

## LOWER

```text
lower(col)
```

Returns the lowercase version of the given string.

## LPAD

```text
lpad(col, 2)
```

Returns the string argument, padded on the left side with the number of spaces specified by the second argument. Notice
that, if the second argument is big, the string will take up a lot of memory. Avoid using a big number as the second
argument, for example, use where clause to filter `SELECT lpad(col, len) from source WHERE len < 999`

## LTRIM

```text
ltrim(col)
```

Removes all leading whitespace (tabs and spaces) from the provided string.

## NUMBYTES

```text
numbytes(col)
```

Returns the number of bytes in the UTF-8 encoding of the provided string.

## REGEXP_MATCHES

```text
regexp_matches(col, regex)
```

Returns true if the string (first argument) contains a match for the regular expression.

## REGEXP_REPLACE

```text
regexp_replace(col, regex, replacement)
```

Replaces all substrings of the specified string value that matches regexp with replacement.

## REGEXP_SUBSTRING

```text
regexp_substring(col, regex)
```

Returns the first substring of the specified string value that matches regexp.

## REVERSE

```text
reverse(col)
```

Returns the reversed string.

## RPAD

```text
rpad(col, 2)
```

Returns the string argument, padded on the right side with the number of spaces specified by the second argument. Notice
that, if the second argument is big, the string will take up a lot of memory. Avoid using a big number as the second
argument, for example, use where clause to filter `SELECT rpad(col, len) from source WHERE len < 999999`

## RTRIM

```text
rtrim(col)
```

Removes all trailing whitespace (tabs and spaces) from the provided string.

## SUBSTRING

```text
substring(col, start, length)
```

Returns the substring of the specified string value starting at the specified index position (0-based, inclusive) for up
to the specified length of characters.

## STARTSWITH

```text
startswith(col, str)
```

Returns a boolean indicating whether the first string argument starts with the second string argument.

## SPLIT_VALUE

```text
split_value(col, str_splitter, index)
```

Split the value of the 1st parameter with the 2nd parameter, and return the value of split array that indexed with the
3rd parameter.

For example, `split_value("/test/device001/message","/",0) AS a`, the returned value of function is empty;

`split_value("/test/device001/message","/",3) AS a`, the returned value of function is `message`.

## TRIM

```text
trim(col)
```

Removes all leading and trailing whitespace (tabs and spaces) from the provided string.

## UPPER

```text
upper(col)
```

Returns the uppercase version of the given string.

## FORMAT

```text
format(col,D[,locale])
```

Formats the number X to a format like '#######.##', rounded to D decimal places, and returns the result as a string,
The optional third parameter enables a locale to be specified to be used for the result number's decimal point.

```sql
SELECT format(12332.1234567, 4,"en_US");
-> '12,332.1235'
```

Depending on the region, the display format of numbers can vary greatly. For example, here are examples to format the number 123456.78 specifically for certain regions:

| **Locale Value** | **Format** |
| ---------------- | ---------- |
| en_US            | 123,456.78 |
| de_DE            | 123.456,78 |
| de_CH            | 123'456.78 |

More regional options:

| Locale Value | Meaning                       |
| ------------ | ----------------------------- |
| ar_AE        | Arabic - United Arab Emirates |
| ar_BH        | Arabic - Bahrain              |
| ar_DZ        | Arabic - Algeria              |
| ar_EG        | Arabic - Egypt                |
| ar_IN        | Arabic - India                |
| ar_IQ        | Arabic - Iraq                 |
| ar_JO        | Arabic - Jordan               |
| ar_KW        | Arabic - Kuwait               |
| ar_LB        | Arabic - Lebanon              |
| ar_LY        | Arabic - Libya                |
| ar_MA        | Arabic - Morocco              |
| ar_OM        | Arabic - Oman                 |
| ar_QA        | Arabic - Qatar                |
| ar_SA        | Arabic - Saudi Arabia         |
| ar_SD        | Arabic - Sudan                |
| ar_SY        | Arabic - Syria                |
| ar_TN        | Arabic - Tunisia              |
| ar_YE        | Arabic - Yemen                |
| be_BY        | Belarusian - Belarus          |
| bg_BG        | Bulgarian - Bulgaria          |
| ca_ES        | Catalan - Spain               |
| cs_CZ        | Czech - Czech Republic        |
| da_DK        | Danish - Denmark              |
| de_AT        | German - Austria              |
| de_BE        | German - Belgium              |
| de_CH        | German - Switzerland          |
| de_DE        | German - Germany              |
| de_LU        | German - Luxembourg           |
| el_GR        | Greek - Greece                |
| en_AU        | English - Australia           |
| en_CA        | English - Canada              |
| en_GB        | English - United Kingdom      |
| en_IN        | English - India               |
| en_NZ        | English - New Zealand         |
| en_PH        | English - Philippines         |
| en_US        | English - United States       |
| en_ZA        | English - South Africa        |
| en_ZW        | English - Zimbabwe            |
| es_AR        | Spanish - Argentina           |
| es_BO        | Spanish - Bolivia             |
| es_CL        | Spanish - Chile               |
| es_CO        | Spanish - Colombia            |
| es_CR        | Spanish - Costa Rica          |
| es_DO        | Spanish - Dominican Republic  |
| es_EC        | Spanish - Ecuador             |
| es_ES        | Spanish - Spain               |
| es_GT        | Spanish - Guatemala           |
| es_HN        | Spanish - Honduras            |
| es_MX        | Spanish - Mexico              |
| es_NI        | Spanish - Nicaragua           |
| es_PA        | Spanish - Panama              |
| es_PE        | Spanish - Peru                |
| es_PR        | Spanish - Puerto Rico         |
| es_PY        | Spanish - Paraguay            |
| es_SV        | Spanish - El Salvador         |
| es_US        | Spanish - United States       |
| es_UY        | Spanish - Uruguay             |
| es_VE        | Spanish - Venezuela           |
| et_EE        | Estonian - Estonia            |
| eu_ES        | Basque - Spain                |
| fi_FI        | Finnish - Finland             |
| fo_FO        | Faroese - Faroe Islands       |
| fr_BE        | French - Belgium              |
| fr_CA        | French - Canada               |
| fr_CH        | French - Switzerland          |
| fr_FR        | French - France               |
| fr_LU        | French - Luxembourg           |
| gl_ES        | Galician - Spain              |
| gu_IN        | Gujarati - India              |
| he_IL        | Hebrew - Israel               |
| hi_IN        | Hindi - India                 |
| hr_HR        | Croatian - Croatia            |
| hu_HU        | Hungarian - Hungary           |
| id_ID        | Indonesian - Indonesia        |
| is_IS        | Icelandic - Iceland           |
| it_CH        | Italian - Switzerland         |
| it_IT        | Italian - Italy               |
| ja_JP        | Japanese - Japan              |
| ko_KR        | Korean - Republic of Korea    |
| lt_LT        | Lithuanian - Lithuania        |
| lv_LV        | Latvian - Latvia              |
| mk_MK        | Macedonian - North Macedonia  |
| mn_MN        | Mongolia - Mongolian          |
| ms_MY        | Malay - Malaysia              |
| nb_NO        | Norwegian(Bokmål) - Norway    |
| nl_BE        | Dutch - Belgium               |
| nl_NL        | Dutch - The Netherlands       |
| no_NO        | Norwegian - Norway            |
| pl_PL        | Polish - Poland               |
| pt_BR        | Portugese - Brazil            |
| pt_PT        | Portugese - Portugal          |
| rm_CH        | Romansh - Switzerland         |
| ro_RO        | Romanian - Romania            |
| ru_RU        | Russian - Russia              |
| ru_UA        | Russian - Ukraine             |
| sk_SK        | Slovak - Slovakia             |
| sl_SI        | Slovenian - Slovenia          |
| sq_AL        | Albanian - Albania            |
| sr_RS        | Serbian - Serbia              |
| sv_FI        | Swedish - Finland             |
| sv_SE        | Swedish - Sweden              |
| ta_IN        | Tamil - India                 |
| te_IN        | Telugu - India                |
| th_TH        | Thai - Thailand               |
| tr_TR        | Turkish - Turkey              |
| uk_UA        | Ukrainian - Ukraine           |
| ur_PK        | Urdu - Pakistan               |
| vi_VN        | Vietnamese - Vietnam          |
| zh_CN        | Chinese - China               |
| zh_HK        | Chinese - Hong Kong           |
| zh_TW        | Chinese - Taiwan              |
