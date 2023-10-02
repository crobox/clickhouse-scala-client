package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait ArithmeticFunctionTokenizer { this: ClickhouseTokenizerModule =>

  protected def tokenizeArithmeticFunction(col: ArithmeticFunction)(implicit ctx: TokenizeContext): String = col match {
    case col: ArithmeticFunctionCol[_] => tokenizeArithmeticFunctionColumn(col)
    case col: ArithmeticFunctionOp[_]  => tokenizeArithmeticFunctionOperator(col)
  }

  protected def tokenizeArithmeticFunctionColumn(col: ArithmeticFunctionCol[_])(implicit ctx: TokenizeContext): String =
    col match {
      case s: Negate[_] => "-" + tokenizeColumn(s.numericCol.column)
      case s: Abs[_]    => s"abs(${tokenizeColumn(s.numericCol.column)})"
    }

  protected def tokenizeArithmeticFunctionOperator(
      col: ArithmeticFunctionOp[_]
  )(implicit ctx: TokenizeContext): String =
    col match {
      case s: Divide[_]       => tokenizeWithOperator(s, "/")
      case s: Gcd[_]          => tokenizeAsFunction(s, "gcd")
      case s: IntDiv[_]       => tokenizeAsFunction(s, "intDiv")
      case s: IntDivOrZero[_] => tokenizeAsFunction(s, "intDivOrZero")
      case s: Lcm[_]          => tokenizeAsFunction(s, "lcm")
      case s: Modulo[_]       => tokenizeWithOperator(s, "%")
      case s: Minus[_]        => tokenizeWithOperator(s, "-")
      case s: Multiply[_]     => tokenizeWithOperator(s, "*")
      case s: Power[_]        => tokenizeAsFunction(s, "pow")
      case s: Plus[_]         => tokenizeWithOperator(s, "+")
    }

  private def tokenizeWithOperator(col: ArithmeticFunctionOp[_],
                                   operator: String)(implicit ctx: TokenizeContext): String =
    tokenizeColumn(col.left.column) + " " + operator + " " + tokenizeColumn(col.right.column)

  private def tokenizeAsFunction(col: ArithmeticFunctionOp[_], fn: String)(implicit ctx: TokenizeContext): String =
    s"$fn(${tokenizeColumn(col.left.column)}, ${tokenizeColumn(col.right.column)})"

}
