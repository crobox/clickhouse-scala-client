package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

trait ComparisonFunctions { self: Magnets =>

  case class ComparisonColumn(left: ConstOrColMagnet[_], operator: String, right: ConstOrColMagnet[_]) extends ExpressionColumn[Boolean](EmptyColumn())

  trait ComparableWith { self: ConstOrColMagnet[_] =>
    def <(other: ConstOrColMagnet[_]) = ComparisonColumn(self, "<", other)
    def >(other: ConstOrColMagnet[_]) = ComparisonColumn(self, ">", other)
    def <>(other: ConstOrColMagnet[_]) = ComparisonColumn(self, "<>", other)
    def isEq(other: ConstOrColMagnet[_]) = ComparisonColumn(self, "=", other)
    def ==(other: ConstOrColMagnet[_]) = ComparisonColumn(self, "=", other)
    def <=(other: ConstOrColMagnet[_]) = ComparisonColumn(self, "<=", other)
    def >=(other: ConstOrColMagnet[_]) = ComparisonColumn(self, ">=", other)
  }

  def equals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]) = ComparisonColumn(col1 , "=", col2)
  def notEquals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]) = ComparisonColumn(col1 , "!=", col2)
  def less(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]) = ComparisonColumn(col1 , "<", col2)
  def greater(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]) = ComparisonColumn(col1 , ">", col2)
  def lessOrEquals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]) = ComparisonColumn(col1 , "<=", col2)
  def greaterOrEquals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]) = ComparisonColumn(col1 , ">=", col2)

  //TODO add 'IS (NOT) LIKE' here?
}
