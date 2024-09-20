package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Column, ExpressionColumn}

trait EmptyFunctions { self: Magnets =>

  sealed abstract class EmptyFunction[+V](val innerCol: Column) extends ExpressionColumn[V](innerCol)

  case class Empty(col: EmptyNonEmptyCol[_])      extends EmptyFunction[Boolean](col.column)
  case class NotEmpty(col: EmptyNonEmptyCol[_])   extends EmptyFunction[Boolean](col.column)
  case class IsNull(col: EmptyNonEmptyCol[_])     extends EmptyFunction[Boolean](col.column)
  case class IsNullable(col: EmptyNonEmptyCol[_]) extends EmptyFunction[Boolean](col.column)
  case class IsNotNull(col: EmptyNonEmptyCol[_])  extends EmptyFunction[Boolean](col.column)
  case class IsNotDistinctFrom(col: EmptyNonEmptyCol[_], other: EmptyNonEmptyCol[_])
      extends EmptyFunction[Boolean](col.column)
  case class IsZeroOrNull(col: EmptyNonEmptyCol[_])                       extends EmptyFunction[Boolean](col.column)
  case class IfNull(col: EmptyNonEmptyCol[_], alt: String)                extends EmptyFunction[Boolean](col.column)
  case class NullIf(col: EmptyNonEmptyCol[_], other: EmptyNonEmptyCol[_]) extends EmptyFunction[Boolean](col.column)
  case class AssumeNotNull(col: EmptyNonEmptyCol[_])                      extends EmptyFunction[Boolean](col.column)
  case class ToNullable(col: EmptyNonEmptyCol[_])                         extends EmptyFunction[Boolean](col.column)

  trait EmptyOps[C] { self: EmptyNonEmptyCol[_] =>

    def empty(): Empty                                                   = Empty(self)
    def notEmpty(): NotEmpty                                             = NotEmpty(self)
    def isNull(): IsNull                                                 = IsNull(self)
    def isNotNull(): IsNotNull                                           = IsNotNull(self)
    def isNullable(): IsNullable                                         = IsNullable(self)
    def isNotDistinctFrom(other: EmptyNonEmptyCol[_]): IsNotDistinctFrom = IsNotDistinctFrom(self, other)
    def isZeroOrNull(): IsZeroOrNull                                     = IsZeroOrNull(self)
    def ifNull(alternative: String): IfNull                              = IfNull(self, alternative)
    def nullIf(other: EmptyNonEmptyCol[_]): NullIf                       = NullIf(self, other)
    def assumeNotNull(): AssumeNotNull                                   = AssumeNotNull(self)
    def toNullable(): ToNullable                                         = ToNullable(self)
  }

  def empty(col: EmptyNonEmptyCol[_]): Empty           = Empty(col: EmptyNonEmptyCol[_])
  def notEmpty(col: EmptyNonEmptyCol[_]): NotEmpty     = NotEmpty(col: EmptyNonEmptyCol[_])
  def isNull(col: EmptyNonEmptyCol[_]): IsNull         = IsNull(col: EmptyNonEmptyCol[_])
  def isNotNull(col: EmptyNonEmptyCol[_]): IsNotNull   = IsNotNull(col: EmptyNonEmptyCol[_])
  def isNullable(col: EmptyNonEmptyCol[_]): IsNullable = IsNullable(col: EmptyNonEmptyCol[_])
  def isNotDistinctFrom(col: EmptyNonEmptyCol[_], other: EmptyNonEmptyCol[_]): IsNotDistinctFrom =
    IsNotDistinctFrom(col: EmptyNonEmptyCol[_], other: EmptyNonEmptyCol[_])
  def isZeroOrNull(col: EmptyNonEmptyCol[_]): IsZeroOrNull          = IsZeroOrNull(col: EmptyNonEmptyCol[_])
  def ifNull(col: EmptyNonEmptyCol[_], alternative: String): IfNull = IfNull(col: EmptyNonEmptyCol[_], alternative)
  def nullIf(col: EmptyNonEmptyCol[_], other: EmptyNonEmptyCol[_]): NullIf =
    NullIf(col: EmptyNonEmptyCol[_], other: EmptyNonEmptyCol[_])
  def assumeNotNull(col: EmptyNonEmptyCol[_]): AssumeNotNull = AssumeNotNull(col: EmptyNonEmptyCol[_])
  def toNullable(col: EmptyNonEmptyCol[_]): ToNullable       = ToNullable(col: EmptyNonEmptyCol[_])
}
