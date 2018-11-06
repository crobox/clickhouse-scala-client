package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

trait ComparisonFunctions { self: Magnets =>

  case class ComparisonColumn(left: Magnet[_], operator: String, right: Magnet[_]) extends ExpressionColumn[Boolean](EmptyColumn())

  trait ComparableWith[M <: Magnet[_]] { self: Magnet[_] =>
    def <(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "<", other)
    def >(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, ">", other)
    def <>(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "!=", other)
    def isEq(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "=", other)
    def ===(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "=", other)
    def !==(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "!=", other)
    def <=(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "<=", other)
    def >=(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, ">=", other)
  }

  def _equals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , "=", col2)
  def notEquals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , "!=", col2)
  def less(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , "<", col2)
  def greater(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , ">", col2)
  def lessOrEquals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , "<=", col2)
  def greaterOrEquals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , ">=", col2)
}
