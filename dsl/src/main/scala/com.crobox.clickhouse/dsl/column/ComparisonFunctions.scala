package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

trait ComparisonFunctions { self: Magnets =>

  case class ComparisonColumn[T <: Magnet](left: T, operator: String, right: T) extends ExpressionColumn[Boolean](EmptyColumn())

  trait ComparableWith[T <: Magnet] { self: T =>
    def <(other: T) = ComparisonColumn[T](self, "<", other)
    def >(other: T) = ComparisonColumn[T](self, ">", other)
    def <>(other: T) = ComparisonColumn[T](self, "<>", other)
    def isEq(other: T) = ComparisonColumn[T](self, "=", other)
    def ==(other: T) = ComparisonColumn[T](self, "=", other)
    def <=(other: T) = ComparisonColumn[T](self, "<=", other)
    def >=(other: T) = ComparisonColumn[T](self, ">=", other)
  }

  def equals[T <: Magnet](col1: T, col2: T) = ComparisonColumn(col1 , "=", col2)
  def notEquals[T <: Magnet](col1: T, col2: T) = ComparisonColumn(col1 , "!=", col2)
  def less[T <: Magnet](col1: T, col2: T) = ComparisonColumn(col1 , "<", col2)
  def greater[T <: Magnet](col1: T, col2: T) = ComparisonColumn(col1 , ">", col2)
  def lessOrEquals[T <: Magnet](col1: T, col2: T) = ComparisonColumn(col1 , "<=", col2)
  def greaterOrEquals[T <: Magnet](col1: T, col2: T) = ComparisonColumn(col1 , ">=", col2)
}
