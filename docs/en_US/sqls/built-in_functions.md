# Functions

Kuiper has many built-in functions for performing calculations on data.

## Aggregate Functions
Aggregate functions perform a calculation on a set of values and return a single value. Aggregate functions can be used as expressions only in the following:
* The select list of a SELECT statement (either a subquery or an outer query).
* A HAVING clause.

| Function | Example     | Description                                    |
| -------- | ----------- | ---------------------------------------------- |
| avg      | avg(col1)   | The average of the values in a group. The null values will be ignored.          |
| count    | count(*)    | The number of items in a group. The null values will be ignored.                 |
| max      | max(col1)   | The maximum value in a group. The null values will be ignored.                  |
| min      | min(col1)   | The minimum value in a group. The null values will be ignored.                   |
| sum      | sum(col1)   | The sum of all the values in a group. The null values will be ignored.           |
| collect   | collect(*), collect(col1)   | Returns an array with all column or the whole record (when the parameter is *) values from the group.    |

## Mathematical Functions
| Function | Example     | Description                                    |
| -------- | ----------- | ---------------------------------------------- |
| abs      | abs(col1)   | The absolute value of a value                  |
| acos     | acos(col1)  | The inverse cosine of a number in radians      |
| asin     | asin(col1)  | The inverse sine of a number in radians        |
| atan     | atan(col1)  | The inverse tangent of a number in radians     |
| atan2    | atan2(col1, col2)  | The angle, in radians,  between the positive x-axis and the (x, y) point defined in the two arguments        |
| bitand   | bitand(col1, col2)  | Performs a bitwise AND on the bit representations of the two Int(-converted) arguments                     |
| bitor    | bitor(col1, col2)  | Performs a bitwise OR of the bit representations of the two arguments                                               |
| bitxor   | bitxor(col1, col2)  | Performs a bitwise XOR on the bit representations of the two Int(-converted) arguments                     |
| bitnot   | bitnot(col1)| Performs a bitwise NOT on the bit representations of the Int(-converted) argument                                          |
| ceil     | ceil(col1)  | Round a value up to the nearest BIGINT value.  |
| cos      | cos(col1)   | Returns the cosine of a number in radians.     |
| cosh     | cosh(col1)  | Returns the hyperbolic cosine of a number in radians.                                                                 |
| exp      | exp(col1)   | Returns e raised to the Decimal argument.      |
| ln       | ln(col1)    | Returns the natural logarithm of the argument. |
| log      | log(col1)   | Returns the base 10 logarithm of the argument. |
| mod      | mod(col1, col2)   | Returns the remainder of the division of the first argument by the second argument.                                  |
| power    | power(x, y) | Pow returns x**y, the base-x exponential of y. |
| rand     | rand()      | Returns a pseudorandom, uniformly distributed double between 0.0 and 1.0.                                              |
| round    | round(col1) | Round a value to the nearest BIGINT value.     |
| sign     | sign(col1)  | Returns the sign of the given number. When the sign of the argument is positive, 1 is returned. When the sign of the argument is negative, -1 is returned. If the argument is 0, 0 is returned.|
| sin      | sin(col1)   | Returns the sine of a number in radians.       |
| sinh     | sinh(col1)  | Returns the hyperbolic sine of a number in radians.                                                                 |
| sqrt     | sqrt(col1)  | Returns the square root of a number.           |
| tan      | tan(col1)   | Returns the tangent of a number in radians.   |
| tanh     | tanh(col1)  | Returns the hyperbolic tangent of a number in radians.     |

## String Functions

| Function | Example     | Description                                    |
| -------- | ----------- | ---------------------------------------------- |
| concat   | concat(col1...)  | Concatenates arrays or strings. This function accepts any number of arguments and returns a String or an Array        |
| endswith | endswith(col1, col2) | Returns a Boolean indicating whether the first String argument ends with the second String argument.              |
| format_time| format_time(col1, format) | Format a datetime to string.    |
| indexof  | indexof(col1, col2)  | Returns the first index (0-based) of the second argument as a substring in the first argument.                    |
| length   | length(col1)| Returns the number of characters in the provided string.                                                                  |
| lower    | lower(col1) | Returns the lowercase version of the given String.                                                                         |
| lpad     | lpad(col1, 2) | Returns the String argument, padded on the left side with the number of spaces specified by the second argument.         |
| ltrim    | ltrim(col1) | Removes all leading whitespace (tabs and spaces) from the provided String.                                                |
| numbytes | numbytes(col1) | Returns the number of bytes in the UTF-8 encoding of the provided string.                                         |
| regexp_matches| regexp_matches(col1, regex) | Returns true if the string (first argument) contains a match for the regular expression.            |
| regexp_replace| regexp_matches(col1, regex, str) | Replaces all occurrences of the second argument (regular expression) in the first argument with the third argument.                                                          |
| regexp_substr| regexp_substr(col1, regex) | Finds the first match of the 2nd parameter (regex) in the first parameter.                            |
| rpad     | rpad(col1, 2) | Returns the String argument, padded on the right side with the number of spaces specified by the second argument.        |
| rtrim    | rtrim(col1) | Removes all trailing whitespace (tabs and spaces) from the provided String.                                                |
| substring| substring(col1, start, end) |  returns the substring of the provided String from the provided Int index (0-based, inclusive) to the end of the String.                                                           |
| startswith| startswith(col1, str) | Returns Boolean, whether the first string argument starts with the second string argument.                  |
| split_value | split_value(col1, str_splitter, index) | Split the value of the 1st parameter with the 2nd parameter, and return the value of split array that indexed with the 3rd parameter.<br />``split_value("/test/device001/message","/",0) AS a``, the returned value of function is empty; <br />``split_value("/test/device001/message","/",3) AS a``, the returned value of function is ``message``; |
| trim      | trim(col1) | Removes all leading and trailing whitespace (tabs and spaces) from the provided String.                                    |
| upper     | upper(col1)| Returns the uppercase version of the given String.|

## Conversion Functions

| Function | Example     | Description                                    |
| -------- | ----------- | ---------------------------------------------- |
| cast     | cast(col,  "bigint") | Converts a value from one data type to another. The supported types includes: bigint, float, string, boolean and datetime(not supported now). |
| chr      | chr(col1)   | Returns the ASCII character that corresponds to the given Int argument                                                   |
| encode   | encode(col1, "base64") |Use the encode function to encode the payload, which potentially might be non-JSON data, into its string representation based on the encoding scheme. Currently, only "base64" econding type is supported.                             |
| trunc    | trunc(dec, int)| Truncates the first argument to the number of Decimal places specified by the second argument. If the second argument is less than zero, it is set to zero. If the second argument is greater than 34, it is set to 34. Trailing zeroes are stripped from the result.       |

## Hashing Functions
| Function | Example     | Description                                    |
| -------- | ----------- | ---------------------------------------------- |
| md5      | md5(col1)   | Hashed value of the argument                   |
| sha1     | sha1(col1)  | Hashed value of the argument                   |
| sha256   | sha256(col1)| Hashed value of the argument                   |
| sha384   | sha384(col1)| Hashed value of the argument                   |
| sha512   | sha512(col1)| Hashed value of the argument                   |
## JSON Functions
| Function | Example     | Description                                    |
| -------- | ----------- | ---------------------------------------------- |
| json_path_exists      | json_path_exists(col1, "$.name")   | Checks whether JSON path returns any item for the specified JSON value. Return bool value.                   |
| json_path_query     | json_path_query(col1, "$.name")  | Gets all items returned by JSON path for the specified JSON value.              |
| json_path_query_first  | json_path_query_first(col1, "$.name")| Gets the first item returned by JSON path for the specified JSON value.                  |

**Please refer to [json path functions](../json_expr.md#json-path-functions) for how to compose a json path.**  

## Other Functions
| Function | Example      | Description                                                  |
| -------- | ------------ | ------------------------------------------------------------ |
| isNull   | isNull(col1) | Returns true if the argument is the Null value.              |
| newuuid  | newuuid()    | Returns a random 16-byte UUID.                               |
| tstamp   | tstamp()     | Returns the current timestamp in milliseconds from 00:00:00 Coordinated Universal Time (UTC), Thursday, 1 January 1970 |
| mqtt     | mqtt(topic)  | Returns the MQTT meta-data of specified key. The current supported keys<br />- topic: return the topic of message.  If there are multiple stream source, then specify the source name in parameter. Such as ``mqtt(src1.topic)``<br />- messageid: return the message id of message. If there are multiple stream source, then specify the source name in parameter. Such as ``mqtt(src2.messageid)`` |
| meta     | meta(topic)  | Returns the meta-data of specified key. The key could be:<br/> - a standalone key if there is only one source in the from clause, such as ``meta(device)``<br />- A qualified key to specify the stream, such as ``meta(src1.device)`` <br />- A key with arrow for multi level meta data, such as ``meta(src1.reading->device->name)`` This assumes reading is a map structure meta data. |
