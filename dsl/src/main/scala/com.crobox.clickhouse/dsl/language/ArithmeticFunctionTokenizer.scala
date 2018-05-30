package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait ArithmeticFunctionTokenizer { this: ClickhouseTokenizerModule =>
  protected def tokenizeArithmeticFunction(col: ArithmeticFunction): String = col match {
    case col: ArithmeticFunctionCol[_] => tokenizeArithmeticFunctionColumn(col)
    case col: ArithmeticFunctionOp[_] => tokenizeArithmeticFunctionOperator(col)
  }

  protected def tokenizeArithmeticFunctionColumn(col: ArithmeticFunctionCol[_]): String = {
    val op = col match {
      case s: Negate => "negate"
      case s: Abs    => "abs"
    }

    fast"$op(${tokenizeColumn(col.numericCol.column)})"
  }

  protected def tokenizeArithmeticFunctionOperator(col: ArithmeticFunctionOp[_]): String = {
    val op = col match {
      case s: Plus         => "plus"
      case s: Minus        => "minus"
      case s: Multiply     => "multiply"
      case s: Divide       => "divide"
      case s: IntDiv       => "intDiv"
      case s: IntDivOrZero => "intDivOrZero"
      case s: Modulo       => "modulo"
      case s: Gcd          => "gcd"
      case s: Lcm          => "lcm"
    }

    fast"$op(${tokenizeColumn(col.left.column)},${tokenizeColumn(col.right.column)})"
  }

}
