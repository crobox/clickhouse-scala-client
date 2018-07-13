package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

trait MathematicalFunctions { self: Magnets =>

  sealed abstract class MathFuncColumn(col: AnyTableColumn) extends ExpressionColumn[Float](col)

  abstract class MathConst                                         extends MathFuncColumn(EmptyColumn())
  abstract class MathTransformation(val numericCol: NumericCol)    extends MathFuncColumn(numericCol.column)

  case class E()  extends MathConst()
  case class Pi() extends MathConst()

  case class Exp(col: NumericCol)              extends MathTransformation(col)
  case class Log(col: NumericCol)              extends MathTransformation(col)
  case class Exp2(col: NumericCol)             extends MathTransformation(col)
  case class Log2(col: NumericCol)             extends MathTransformation(col)
  case class Exp10(col: NumericCol)            extends MathTransformation(col)
  case class Log10(col: NumericCol)            extends MathTransformation(col)
  case class Sqrt(col: NumericCol)             extends MathTransformation(col)
  case class Cbrt(col: NumericCol)             extends MathTransformation(col)
  case class Erf(col: NumericCol)              extends MathTransformation(col)
  case class Erfc(col: NumericCol)             extends MathTransformation(col)
  case class Lgamma(col: NumericCol)           extends MathTransformation(col)
  case class Tgamma(col: NumericCol)           extends MathTransformation(col)
  case class Sin(col: NumericCol)              extends MathTransformation(col)
  case class Cos(col: NumericCol)              extends MathTransformation(col)
  case class Tan(col: NumericCol)              extends MathTransformation(col)
  case class Asin(col: NumericCol)             extends MathTransformation(col)
  case class Acos(col: NumericCol)             extends MathTransformation(col)
  case class Atan(col: NumericCol)             extends MathTransformation(col)
  case class Pow(x: NumericCol, y: NumericCol) extends MathTransformation(x)

  def e(col: NumericCol)                = E()
  def pi(col: NumericCol)               = Pi()
  def exp(col: NumericCol)              = Exp(col)
  def log(col: NumericCol)              = Log(col)
  def exp2(col: NumericCol)             = Exp2(col)
  def log2(col: NumericCol)             = Log2(col)
  def exp10(col: NumericCol)            = Exp10(col)
  def log10(col: NumericCol)            = Log10(col)
  def sqrt(col: NumericCol)             = Sqrt(col)
  def cbrt(col: NumericCol)             = Cbrt(col)
  def erf(col: NumericCol)              = Erf(col)
  def erfc(col: NumericCol)             = Erfc(col)
  def lgamma(col: NumericCol)           = Lgamma(col)
  def tgamma(col: NumericCol)           = Tgamma(col)
  def sin(col: NumericCol)              = Sin(col)
  def cos(col: NumericCol)              = Cos(col)
  def tan(col: NumericCol)              = Tan(col)
  def asin(col: NumericCol)             = Asin(col)
  def acos(col: NumericCol)             = Acos(col)
  def atan(col: NumericCol)             = Atan(col)
  def pow(x: NumericCol, y: NumericCol) = Pow(x, y)

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
