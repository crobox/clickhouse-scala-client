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
      case s: Negate[_] => "negate"
      case s: Abs[_]    => "abs"
    }

    fast"$op(${tokenizeColumn(col.numericCol.column)})"
  }

  protected def tokenizeArithmeticFunctionOperator(col: ArithmeticFunctionOp[_]): String = {
    val op = col match {
      case s: Plus[_]         => "plus"
      case s: Minus[_]        => "minus"
      case s: Multiply[_]     => "multiply"
      case s: Divide[_]       => "divide"
      case s: IntDiv[_]       => "intDiv"
      case s: IntDivOrZero[_] => "intDivOrZero"
      case s: Modulo[_]       => "modulo"
      case s: Gcd[_]          => "gcd"
      case s: Lcm[_]          => "lcm"
    }

    fast"$op(${tokenizeColumn(col.left.column)},${tokenizeColumn(col.right.column)})"
  }

}
