package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait MathematicalFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeMathematicalFunction(col: MathFuncColumn)(implicit ctx: TokenizeContext): String = col match {
    case Pow(x: NumericCol[_], y: NumericCol[_]) => s"pow(${tokenizeColumn(x.column)},${tokenizeColumn(y.column)})"
    case c: MathConst                            => tokenizeMathConst(c)
    case c: MathTransformation                   => tokenizeMathTransformation(c)

  }

  private def tokenizeMathConst(col: MathConst): String = col match {
    case c: E  => "e()"
    case c: Pi => "pi()"
  }

  private def tokenizeMathTransformation(col: MathTransformation)(implicit ctx: TokenizeContext): String = {
    val command = col match {
      case Exp(_)    => "exp"
      case Log(_)    => "log"
      case Exp2(_)   => "exp2"
      case Log2(_)   => "log2"
      case Exp10(_)  => "exp10"
      case Log10(_)  => "log10"
      case Sqrt(_)   => "sqrt"
      case Cbrt(_)   => "cbrt"
      case Erf(_)    => "erf"
      case Erfc(_)   => "erfc"
      case Lgamma(_) => "lgamma"
      case Tgamma(_) => "tgamma"
      case Sin(_)    => "sin"
      case Cos(_)    => "cos"
      case Tan(_)    => "tan"
      case Asin(_)   => "asin"
      case Acos(_)   => "acos"
      case Atan(_)   => "atan"
    }

    s"$command(${tokenizeColumn(col.numericCol.column)})"
  }

}
