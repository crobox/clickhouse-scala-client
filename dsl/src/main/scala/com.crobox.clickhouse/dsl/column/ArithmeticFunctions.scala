package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn, TableColumn}

trait ArithmeticFunctions { self: Magnets =>

  sealed trait ArithmeticFunction

  abstract class ArithmeticFunctionCol[V](val numericCol: NumericCol)
    extends ExpressionColumn[V](numericCol.column)
  with ArithmeticFunction

  abstract class ArithmeticFunctionOp[V](val left: AddSubtractAble, val right: AddSubtractAble)
      extends ExpressionColumn[V](EmptyColumn())
        with ArithmeticFunction

  trait AddSubtractOps { self: AddSubtractAble =>
    def +(other: AddSubtractAble) = Plus(this, other)
    def -(other: AddSubtractAble) = Minus(this, other)
  }

  trait ArithmeticOps { self: NumericCol =>
    def *(other: NumericCol) = Multiply(this, other)
    def /(other: NumericCol) = Divide(this, other)
    def %(other: NumericCol) = Modulo(this, other)
  }

  import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._

  plus(1,3)

  case class Plus(l: AddSubtractAble, r: AddSubtractAble)    extends ArithmeticFunctionOp(l, r)
  case class Minus(l: AddSubtractAble, r: AddSubtractAble)   extends ArithmeticFunctionOp(l, r)
  case class Multiply(l: NumericCol, r: NumericCol)     extends ArithmeticFunctionOp(l, r)
  case class Divide(l: NumericCol, r: NumericCol)       extends ArithmeticFunctionOp(l, r)
  case class IntDiv(l: NumericCol, r: NumericCol)       extends ArithmeticFunctionOp(l, r)
  case class IntDivOrZero(l: NumericCol, r: NumericCol) extends ArithmeticFunctionOp(l, r)
  case class Modulo(l: NumericCol, r: NumericCol)       extends ArithmeticFunctionOp(l, r)
  case class Gcd(l: NumericCol, r: NumericCol)          extends ArithmeticFunctionOp(l, r)
  case class Lcm(l: NumericCol, r: NumericCol)          extends ArithmeticFunctionOp(l, r)

  case class Negate(t: NumericCol) extends ArithmeticFunctionCol(t)
  case class Abs(t: NumericCol)    extends ArithmeticFunctionCol(t)

  //trait ArithmeticFunctionsDsl {

    def plus(left: AddSubtractAble, right: AddSubtractAble)    = Plus(left, right)
    def minus(left: AddSubtractAble, right: AddSubtractAble)   = Minus(left, right)
    def multiply(left: NumericCol, right: NumericCol)     = Multiply(left, right)
    def divide(left: NumericCol, right: NumericCol)       = Divide(left, right)
    def intDiv(left: NumericCol, right: NumericCol)       = IntDiv(left, right)
    def intDivOrZero(left: NumericCol, right: NumericCol) = IntDivOrZero(left, right)
    def modulo(left: NumericCol, right: NumericCol)       = Modulo(left, right)
    def gcd(left: NumericCol, right: NumericCol)          = Gcd(left, right)
    def lcm(left: NumericCol, right: NumericCol)          = Lcm(left, right)
    def negate(targetColumn: NumericCol)                  = Negate(targetColumn)
    def abs(targetColumn: NumericCol)                     = Abs(targetColumn)
 // }
}
