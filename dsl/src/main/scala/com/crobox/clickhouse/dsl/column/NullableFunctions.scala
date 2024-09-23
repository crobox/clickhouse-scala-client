package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Column, ExpressionColumn, TableColumn}

trait NullableFunctions {  self: Magnets =>

  sealed trait NullableFunction

  sealed abstract class AbsNullableFunction(val innerCol: Column) extends TableColumn(innerCol.name) with NullableFunction

  case class IsNull(col: ConstOrColMagnet[_])     extends ExpressionColumn[Boolean](col.column)
  case class IsNullable(col: ConstOrColMagnet[_]) extends ExpressionColumn[Boolean](col.column)
  case class IsNotNull(col: ConstOrColMagnet[_])  extends ExpressionColumn[Boolean](col.column)
  case class IsZeroOrNull(col: ConstOrColMagnet[_]) extends ExpressionColumn[Boolean](col.column)

  case class IfNull(col: ConstOrColMagnet[_], alt: ConstOrColMagnet[_])   extends AbsNullableFunction(col.column)
  case class NullIf(col: ConstOrColMagnet[_], other: ConstOrColMagnet[_]) extends AbsNullableFunction(col.column)

  case class AssumeNotNull(col: ConstOrColMagnet[_])                      extends AbsNullableFunction(col.column)
  case class ToNullable(col: ConstOrColMagnet[_])                         extends AbsNullableFunction(col.column)

  trait NullableOps {
    self: ConstOrColMagnet[_] =>

    def isNull(): IsNull                                                 = IsNull(self)
    def isNullable(): IsNullable                                         = IsNullable(self)
    def isNotNull(): IsNotNull                                           = IsNotNull(self)
    def isZeroOrNull(): IsZeroOrNull                                     = IsZeroOrNull(self)

    def ifNull(alternative: ConstOrColMagnet[_]): IfNull                 = IfNull(self, alternative)
    def nullIf(other: ConstOrColMagnet[_]): NullIf                       = NullIf(self, other)
    def assumeNotNull(): AssumeNotNull                                   = AssumeNotNull(self)
    def toNullable(): ToNullable                                         = ToNullable(self)

  }

  def isNull(col: ConstOrColMagnet[_]): IsNull         = IsNull(col)
  def isNullable(col: ConstOrColMagnet[_]): IsNullable = IsNullable(col)
  def isNotNull(col: ConstOrColMagnet[_]): IsNotNull   = IsNotNull(col)

  def isZeroOrNull(col: ConstOrColMagnet[_]): IsZeroOrNull          = IsZeroOrNull(col)
  def ifNull(col: ConstOrColMagnet[_], alternative: ConstOrColMagnet[_]): IfNull = IfNull(col, alternative)
  def nullIf(col: ConstOrColMagnet[_], other: ConstOrColMagnet[_]): NullIf = NullIf(col, other)
  def assumeNotNull(col: ConstOrColMagnet[_]): AssumeNotNull = AssumeNotNull(col)
  def toNullable(col: ConstOrColMagnet[_]): ToNullable       = ToNullable(col)
}
