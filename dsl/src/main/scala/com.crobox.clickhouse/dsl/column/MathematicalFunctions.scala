package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

trait MathematicalFunctions { self: Magnets =>

  abstract class MathConst[V] extends ExpressionColumn[Long](EmptyColumn())
  abstract class MathFunctionCol(val numericCol: NumericCol) extends ExpressionColumn[Long](numericCol.column)


  case class E() extends MathConst()
  case class Pi() extends MathConst()
  
  case class Exp(col: NumericCol) extends MathFunctionCol(col)//TODO: Correct types
  case class Log(col: NumericCol) extends MathFunctionCol(col)
  case class Exp2(col: NumericCol) extends MathFunctionCol(col)
  case class Log2(col: NumericCol) extends MathFunctionCol(col)
  case class Exp10(col: NumericCol) extends MathFunctionCol(col)
  case class Log10(col: NumericCol) extends MathFunctionCol(col)
  case class Sqrt(col: NumericCol) extends MathFunctionCol(col)
  case class Cbrt(col: NumericCol) extends MathFunctionCol(col)
  case class Erf(col: NumericCol) extends MathFunctionCol(col)
  case class Erfc(col: NumericCol) extends MathFunctionCol(col)
  case class Lgamma(col: NumericCol) extends MathFunctionCol(col)
  case class Tgamma(col: NumericCol) extends MathFunctionCol(col)
  case class Sin(col: NumericCol) extends MathFunctionCol(col)
  case class Cos(col: NumericCol) extends MathFunctionCol(col)
  case class Tan(col: NumericCol) extends MathFunctionCol(col)
  case class Asin(col: NumericCol) extends MathFunctionCol(col)
  case class Acos(col: NumericCol) extends MathFunctionCol(col)
  case class Atan(col: NumericCol) extends MathFunctionCol(col)
  case class Pow(x: NumericCol,y: NumericCol) extends MathFunctionCol(x)

  def e(col: NumericCol) = E()
  def pi(col: NumericCol) = Pi()
  def exp(col: NumericCol) = Exp(col)
  def log(col: NumericCol) = Log(col)
  def exp2(col: NumericCol) = Exp2(col)
  def log2(col: NumericCol) = Log2(col)
  def exp10(col: NumericCol) = Exp10(col)
  def log10(col: NumericCol) = Log10(col)
  def sqrt(col: NumericCol) = Sqrt(col)
  def cbrt(col: NumericCol) = Cbrt(col)
  def erf(col: NumericCol) = Erf(col)
  def erfc(col: NumericCol) = Erfc(col)
  def lgamma(col: NumericCol) = Lgamma(col)
  def tgamma(col: NumericCol) = Tgamma(col)
  def sin(col: NumericCol) = Sin(col)
  def cos(col: NumericCol) = Cos(col)
  def tan(col: NumericCol) = Tan(col)
  def asin(col: NumericCol) = Asin(col)
  def acos(col: NumericCol) = Acos(col)
  def atan(col: NumericCol) = Atan(col)
  def pow(x: NumericCol, y: NumericCol) = Pow(x,y)

  /*
e()
pi()
exp(x)
log(x)
exp2(x)
log2(x)
exp10(x)
log10(x)
sqrt(x)
cbrt(x)
erf(x)
erfc(x)
lgamma(x)
tgamma(x)
sin(x)
cos(x)
tan(x)
asin(x)
acos(x)
atan(x)
pow(x, y)
    */

}
