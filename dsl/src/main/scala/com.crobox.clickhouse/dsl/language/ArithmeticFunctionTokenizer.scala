package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait ArithmeticFunctionTokenizer { this: ClickhouseTokenizerModule =>
  protected def tokenizeArithmeticFunction(col: ArithmeticFunction): String = col match {
    case col: ArithmeticFunctionCol[_] => tokenizeArithmeticFunctionColumn(col)
    case col: ArithmeticFunctionOp[_] => tokenizeArithmeticFunctionOperator(col)
  }

  protected def tokenizeArithmeticFunctionColumn(col: ArithmeticFunctionCol[_]): String =
    col match {
      case s: Negate[_] => "-" + tokenizeColumn(s.numericCol.column)
      case s: Abs[_]    => fast"abs(${tokenizeColumn(s.numericCol.column)})"
    }

  protected def tokenizeArithmeticFunctionOperator(col: ArithmeticFunctionOp[_]): String =
    col match {
      case s: Plus[_]         => tokenizeWithOperator(s, "+")
      case s: Minus[_]        => tokenizeWithOperator(s, "-")
      case s: Multiply[_]     => tokenizeWithOperator(s, "*")
      case s: Divide[_]       => tokenizeWithOperator(s, "/")
      case s: Modulo[_]       => tokenizeWithOperator(s, "%")
      case s: IntDiv[_]       => tokenizeAsFunction(s, "intDiv")
      case s: IntDivOrZero[_] => tokenizeAsFunction(s, "intDivOrZero")
      case s: Gcd[_]          => tokenizeAsFunction(s, "gcd")
      case s: Lcm[_]          => tokenizeAsFunction(s, "lcm")
    }

  private def tokenizeWithOperator(col: ArithmeticFunctionOp[_], operator: String) =
    tokenizeColumn(col.left.column) + " " + operator + " " + tokenizeColumn(col.right.column)

  private def tokenizeAsFunction(col: ArithmeticFunctionOp[_], fn: String) =
    fast"$fn(${tokenizeColumn(col.left.column)}, ${tokenizeColumn(col.right.column)})"

}
