package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

object ArithmeticFunctions {
  abstract class ArithmeticFunctionCol[V](val targetColumn: AnyTableColumn) extends ExpressionColumn[V](targetColumn)

  abstract class ArithmeticFunctionOp[V](val left: AnyTableColumn, val right: AnyTableColumn)
      extends ExpressionColumn[V](EmptyColumn())

  case class Plus(l: AnyTableColumn, r: AnyTableColumn)         extends ArithmeticFunctionOp(l, r)
  case class Minus(l: AnyTableColumn, r: AnyTableColumn)        extends ArithmeticFunctionOp(l, r)
  case class Multiply(l: AnyTableColumn, r: AnyTableColumn)     extends ArithmeticFunctionOp(l, r)
  case class Divide(l: AnyTableColumn, r: AnyTableColumn)       extends ArithmeticFunctionOp(l, r)
  case class IntDiv(l: AnyTableColumn, r: AnyTableColumn)       extends ArithmeticFunctionOp(l, r)
  case class IntDivOrZero(l: AnyTableColumn, r: AnyTableColumn) extends ArithmeticFunctionOp(l, r)
  case class Modulo(l: AnyTableColumn, r: AnyTableColumn)       extends ArithmeticFunctionOp(l, r)
  case class Gcd(l: AnyTableColumn, r: AnyTableColumn)          extends ArithmeticFunctionOp(l, r)
  case class Lcm(l: AnyTableColumn, r: AnyTableColumn)          extends ArithmeticFunctionOp(l, r)

  case class Negate(t: AnyTableColumn) extends ArithmeticFunctionCol(t)
  case class Abs(t: AnyTableColumn)    extends ArithmeticFunctionCol(t)

  trait ArithmeticFunctionsDsl {

    implicit class ColumnSymbolOps(column: AnyTableColumn) {
      def +(other: AnyTableColumn) = Plus(column, other)

      def -(other: AnyTableColumn) = Minus(column, other)

      def *(other: AnyTableColumn) = Multiply(column, other)

      def /(other: AnyTableColumn) = Divide(column, other)

      def %(other: AnyTableColumn) = Modulo(column, other)
    }

    def plus(left: AnyTableColumn, right: AnyTableColumn) = Plus(left, right)

    def minus(left: AnyTableColumn, right: AnyTableColumn) = Minus(left, right)

    def multiply(left: AnyTableColumn, right: AnyTableColumn) = Multiply(left, right)

    def divide(left: AnyTableColumn, right: AnyTableColumn) = Divide(left, right)

    def intDiv(left: AnyTableColumn, right: AnyTableColumn) = IntDiv(left, right)

    def intDivOrZero(left: AnyTableColumn, right: AnyTableColumn) = IntDivOrZero(left, right)

    def modulo(left: AnyTableColumn, right: AnyTableColumn) = Modulo(left, right)

    def gcd(left: AnyTableColumn, right: AnyTableColumn) = Gcd(left, right)

    def lcm(left: AnyTableColumn, right: AnyTableColumn) = Lcm(left, right)

    def negate(targetColumn: AnyTableColumn) = Negate(targetColumn)

    def abs(targetColumn: AnyTableColumn) = Abs(targetColumn)
  }
}
