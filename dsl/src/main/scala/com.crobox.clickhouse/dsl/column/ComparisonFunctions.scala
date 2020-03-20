package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

import scala.reflect.ClassTag

trait ComparisonFunctions { self: Magnets =>

  case class ComparisonColumn(left: Magnet[_], operator: String, right: Magnet[_]) extends ExpressionColumn[Boolean](EmptyColumn)

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

  def _equals[L,R: ClassTag](col1: ConstOrColMagnet[L], col2: ConstOrColMagnet[R]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , "=", col2)
  def notEquals[L,R: ClassTag](col1: ConstOrColMagnet[L], col2: ConstOrColMagnet[R]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , "!=", col2)
  def less[L,R: ClassTag](col1: ConstOrColMagnet[L], col2: ConstOrColMagnet[R]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , "<", col2)
  def greater[L,R: ClassTag](col1: ConstOrColMagnet[L], col2: ConstOrColMagnet[R]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , ">", col2)
  def lessOrEquals[L,R: ClassTag](col1: ConstOrColMagnet[L], col2: ConstOrColMagnet[R]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , "<=", col2)
  def greaterOrEquals[L,R: ClassTag](col1: ConstOrColMagnet[L], col2: ConstOrColMagnet[R]): ExpressionColumn[Boolean] = ComparisonColumn(col1 , ">=", col2)
}
