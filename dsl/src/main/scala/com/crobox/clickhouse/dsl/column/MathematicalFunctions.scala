package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Column, EmptyColumn, ExpressionColumn}

trait MathematicalFunctions { self: Magnets =>

  sealed abstract class MathFuncColumn(col: Column) extends ExpressionColumn[Double](col)

  abstract class MathConst                                         extends MathFuncColumn(EmptyColumn)
  abstract class MathTransformation(val numericCol: NumericCol[_]) extends MathFuncColumn(numericCol.column)

  case class E()  extends MathConst()
  case class Pi() extends MathConst()

  case class Exp(col: NumericCol[_])                 extends MathTransformation(col)
  case class Log(col: NumericCol[_])                 extends MathTransformation(col)
  case class Exp2(col: NumericCol[_])                extends MathTransformation(col)
  case class Log2(col: NumericCol[_])                extends MathTransformation(col)
  case class Exp10(col: NumericCol[_])               extends MathTransformation(col)
  case class Log10(col: NumericCol[_])               extends MathTransformation(col)
  case class Sqrt(col: NumericCol[_])                extends MathTransformation(col)
  case class Cbrt(col: NumericCol[_])                extends MathTransformation(col)
  case class Erf(col: NumericCol[_])                 extends MathTransformation(col)
  case class Erfc(col: NumericCol[_])                extends MathTransformation(col)
  case class Lgamma(col: NumericCol[_])              extends MathTransformation(col)
  case class Tgamma(col: NumericCol[_])              extends MathTransformation(col)
  case class Sin(col: NumericCol[_])                 extends MathTransformation(col)
  case class Cos(col: NumericCol[_])                 extends MathTransformation(col)
  case class Tan(col: NumericCol[_])                 extends MathTransformation(col)
  case class Asin(col: NumericCol[_])                extends MathTransformation(col)
  case class Acos(col: NumericCol[_])                extends MathTransformation(col)
  case class Atan(col: NumericCol[_])                extends MathTransformation(col)
  case class Pow(x: NumericCol[_], y: NumericCol[_]) extends MathTransformation(x)

  def e()                                     = E()
  def pi()                                    = Pi()
  def exp(col: NumericCol[_])                 = Exp(col)
  def log(col: NumericCol[_])                 = Log(col)
  def exp2(col: NumericCol[_])                = Exp2(col)
  def log2(col: NumericCol[_])                = Log2(col)
  def exp10(col: NumericCol[_])               = Exp10(col)
  def log10(col: NumericCol[_])               = Log10(col)
  def sqrt(col: NumericCol[_])                = Sqrt(col)
  def cbrt(col: NumericCol[_])                = Cbrt(col)
  def erf(col: NumericCol[_])                 = Erf(col)
  def erfc(col: NumericCol[_])                = Erfc(col)
  def lgamma(col: NumericCol[_])              = Lgamma(col)
  def tgamma(col: NumericCol[_])              = Tgamma(col)
  def sin(col: NumericCol[_])                 = Sin(col)
  def cos(col: NumericCol[_])                 = Cos(col)
  def tan(col: NumericCol[_])                 = Tan(col)
  def asin(col: NumericCol[_])                = Asin(col)
  def acos(col: NumericCol[_])                = Acos(col)
  def atan(col: NumericCol[_])                = Atan(col)
  def pow(x: NumericCol[_], y: NumericCol[_]) = Pow(x, y)
}
