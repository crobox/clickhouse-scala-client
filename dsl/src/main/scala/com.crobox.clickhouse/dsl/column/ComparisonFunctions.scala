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

  /*
equals
notEquals
less
greater
lessOrEquals
greaterOrEquals
   */
}
