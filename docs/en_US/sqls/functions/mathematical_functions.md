# Mathematical Functions

Mathematical functions perform many of the common mathematical operations. They take numeric input and return numeric
output.

## ABS

```text
abs(col)
```

The absolute value of a value.

## ACOS

```text
acos(col)
```

The inverse cosine of a number of radians.

## ASIN

```text
asin(col)
```

The inverse sine of a number of radians.

## ATAN

```text
atan(col)
```

The inverse tangent of a number of radians.

## ATAN2

```text
atan2(col1, col2)
```

The angle, in radians, between the positive x-axis and the (x, y) point defined in the two arguments.

## BITAND

```text
bitand(col1, col2)
```

Performs a bitwise AND on the bit representations of the two Int(-converted) arguments.

## BITOR

```text
bitor(col1, col2)
```

Performs a bitwise OR of the bit representations of the two arguments.

## BITXOR

```text
bitxor(col1, col2)
```

Performs a bitwise XOR on the bit representations of the two Int(-converted) arguments.

## BITNOT

```text
bitnot(col1)
```

Performs a bitwise NOT on the bit representations of the Int(-converted) argument.

## CEIL

`CEIL()` is a synonym for [`CEILING()`](#ceiling).

## CEILING

```text
ceiling(col)
```

The smallest integer value that is greater than or equal to the argument.

## COS

```text
cos(col)
```

The cosine of a number of radians.

## COSH

```text
cosh(col)
```

The hyperbolic cosine of a number.

## EXP

```text
exp(col)
```

Returns Euler's number e raised to the power of a double value.

## FLOOR

```text
floor(col)
```

Returns the largest integer value not greater than X.

## LN

```text
ln(col)
```

Returns the natural logarithm of a double value.

## LOG

```text
log(col)

or

log(b, col)
```

If called with one argument, the function returns the decimal logarithm of X. If X is less than or equal to 0, the function returns nil; if called with two arguments, the function returns the base B logarithm of X. Returns nil if X is less than or equal to 0, or if B is less than or equal to 1.

## MOD

```text
mod(col1, col2)
```

Returns the remainder of the division of the first argument by the second argument.

## PI

```text
pi()
```

Returns the value of π (pi).

## POW

`POW()` is a synonym for [`POWER()`](#power).

## POWER

```text
power(col1, col2)
```

Returns the value of the first argument raised to the power of the second argument.

## RAND

```text
rand()
```

Returns a random number between 0.0 (inclusive) and 1.0 (exclusive).

## ROUND

```text
round(v, [s])
```

Round to s decimal places. If s is not specified, round to nearest integer.

```text
round(42.4)  -> 42
round(42.4382, 2) -> 42.44
```

## SIGN

```text
sign(col)
```

Returns the signum function of the argument. When the sign of the argument is positive, 1 is returned. When the sign of
the argument is negative, -1 is returned. If the argument is 0, 0 is returned.

## SIN

```text
sin(col)
```

The sine of a numb[multi_column_functions.md](multi_column_functions.md)er in radians.

## SINH

```text
sinh(col)
```

The hyperbolic sine of a number.

## SQRT

```text
sqrt(col)
```

Returns the positive square root of a double value.

## TAN

```text
tan(col)
```

The tangent of a number of radians.

## TANH

```text
tanh(col)
```

The hyperbolic tangent of a number.

## COT

```text
cot(col)
```

Returns the cotangent of a number.

## RADIANS

```text
radians(col)
```

converted from degrees to radians.

## DEGREES

```text
degrees(col)
```

converted from radians to degrees

## CONV

```text
conv(N,from_base,to_base)
```

converts numbers between different number bases. Returns a string representation of the number N, converted
from base from_base to base to_base. Returns NULL if any argument is NULL. The argument N is interpreted as an integer,
but may be specified as an integer or a string. The minimum base is 2 and the maximum base is 36.

```sql
ekuiper> select conv('a',16,2);
        -> '1010'
ekuiper> select conv('6E',18,8);
        -> '172'
ekuiper> select conv(-17,10,-18);
        -> '-H'
```
