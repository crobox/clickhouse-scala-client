package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
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

  def equals(col1: AnyTableColumn, col2: AnyTableColumn) = ComparisonColumn(col1 , "=", col2)
  def notEquals(col1: AnyTableColumn, col2: AnyTableColumn) = ComparisonColumn(col1 , "!=", col2)
  def less(col1: AnyTableColumn, col2: AnyTableColumn) = ComparisonColumn(col1 , "<", col2)
  def greater(col1: AnyTableColumn, col2: AnyTableColumn) = ComparisonColumn(col1 , ">", col2)
  def lessOrEquals(col1: AnyTableColumn, col2: AnyTableColumn) = ComparisonColumn(col1 , "<=", col2)
  def greaterOrEquals(col1: AnyTableColumn, col2: AnyTableColumn) = ComparisonColumn(col1 , ">=", col2)

}
