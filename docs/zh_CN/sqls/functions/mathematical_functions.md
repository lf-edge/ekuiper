# 数学函数

数学函数执行许多常见的数学运算。它们接受数字输入并返回数字输出。

## ABS

```text
abs(col)
```

返回参数的绝对值。

## ACOS

```text
acos(col)
```

返回弧度数的反余弦值。

## ASIN

```text
asin(col)
```

返回弧度数的反正弦值。

## ATAN

```text
atan(col)
```

返回弧度数的反正切值。

## ATAN2

```text
atan2(col1, col2)
```

返回正x轴与两个自变量中定义的（x，y）点之间的弧度角。

## BITAND

```text
bitand(col1, col2)
```

对两个 Int 参数执行按位与运算。

## BITOR

```text
bitor(col1, col2)
```

对两个 Int 参数执行按位或运算。

## BITXOR

```text
bitxor(col1, col2)
```

对两个 Int 参数执行按位异或运算。

## BITNOT

```text
bitnot(col)
```

在 Int 参数的执行按位非运算。

## CEIL

`CEIL()` 是 [`CEILING()`](#ceiling) 的别名。

## CEILING

```text
ceiling(col)
```

将值舍入到最接近的 BIGINT 值。

## COS

```text
cos(col)
```

返回以弧度为单位的数字的余弦值。

## COSH

```text
cosh(col)
```

返回弧度数的双曲余弦值。

## EXP

```text
exp(col)
```

返回小数点参数的 e。

## FLOOR

```text
floor(col)
```

返回小于 X 的最大整数值。

## LN

```text
ln(col)
```

返回参数的自然对数。

## LOG

```text
log(col)

or

log(b, col)
```

如果使用一个参数调用，该函数将返回 X 的十进制对数。如果 X 小于或等于 0，则该函数返回 nil；如果使用两个参数调用，该函数返回 X 的 B 底对数。如果 X 小于或等于 0，或者 B 小于或等于 1，则返回 nil。

## MOD

```text
mod(col1, col2)
```

返回第一个参数除以第二个参数的余数。

## PI

```text
pi()
```

返回 π (pi) 的值。

## POW

`POW()` 是函数 [`POWER()`](#power) 的别名。

## POWER

```text
power(x, y)
```

返回 x 的 y 次方。

## RAND

```text
rand()
```

返回一个伪随机数，其均匀分布在0.0和1.0之间。

## ROUND

```text
round(col)
```

将值四舍五入到最接近的 BIGINT 值。

## SIGN

```text
sign(col)
```

返回给定数字的符号。 当参数的符号为正时，将返回1。 当参数的符号为负数时，返回-1。 如果参数为0，则返回0。

## SIN

```text
sin(col)
```

返回以弧度为单位的数字的正弦值。

## SINH

```text
sinh(col)
```

返回弧度数的双曲正弦值。

## SQRT

```text
sqrt(col)
```

返回参数的平方根。

## TAN

```text
tan(col)
```

返回以弧度为单位的数字的正切值。

## TANH

```text
tanh(col)
```

返回弧度数的双曲正切值。
