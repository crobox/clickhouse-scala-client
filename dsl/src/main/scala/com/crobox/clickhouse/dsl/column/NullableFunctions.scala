package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Column, ExpressionColumn}

trait NullableFunctions { self: Magnets =>

  sealed trait NullableFunction

  sealed abstract class AbsNullableFunction[+V](val innerCol: Column)
      extends ExpressionColumn[V](innerCol)
      with NullableFunction

  case class IsNull(col: ConstOrColMagnet[_])       extends AbsNullableFunction[Boolean](col.column)
  case class IsNullable(col: ConstOrColMagnet[_])   extends AbsNullableFunction[Boolean](col.column)
  case class IsNotNull(col: ConstOrColMagnet[_])    extends AbsNullableFunction[Boolean](col.column)
  case class IsZeroOrNull(col: ConstOrColMagnet[_]) extends AbsNullableFunction[Boolean](col.column)

  /**
   * The four that hand their column back keep its type, so the result can still be fed to a function that asks for one.
   * Without it they were `AbsNullableFunction[Nothing]`, and `divide(x, nullIf(y, 0.0))` had no numeric conversion to
   * pick -- every one of them applies to `Nothing`.
   *
   * The result follows the *first* argument. `nullIf` is that in ClickHouse too; `ifNull` is really the supertype of
   * both, but inferring one would type `ifNull(price, 0)` as `AnyVal` and put the caller right back where they started.
   * So the alternative stays untyped and unchecked against the column.
   */
  case class AssumeNotNull[+V](col: ConstOrColMagnet[V]) extends AbsNullableFunction[V](col.column)
  case class ToNullable[+V](col: ConstOrColMagnet[V])    extends AbsNullableFunction[V](col.column)

  case class IfNull[+V](col: ConstOrColMagnet[V], alt: ConstOrColMagnet[_])   extends AbsNullableFunction[V](col.column)
  case class NullIf[+V](col: ConstOrColMagnet[V], other: ConstOrColMagnet[_]) extends AbsNullableFunction[V](col.column)

  trait NullableOps[+C] {
    self: ConstOrColMagnet[C] =>

    def isNull(): IsNull                                    = IsNull(self)
    def isNullable(): IsNullable                            = IsNullable(self)
    def isNotNull(): IsNotNull                              = IsNotNull(self)
    def isZeroOrNull(): IsZeroOrNull                        = IsZeroOrNull(self)
    def ifNull(alternative: ConstOrColMagnet[_]): IfNull[C] = IfNull(self, alternative)
    def nullIf(other: ConstOrColMagnet[_]): NullIf[C]       = NullIf(self, other)
    def assumeNotNull(): AssumeNotNull[C]                   = AssumeNotNull(self)
    def toNullable(): ToNullable[C]                         = ToNullable(self)

  }

  def isNull(col: ConstOrColMagnet[_]): IsNull             = IsNull(col)
  def isNullable(col: ConstOrColMagnet[_]): IsNullable     = IsNullable(col)
  def isNotNull(col: ConstOrColMagnet[_]): IsNotNull       = IsNotNull(col)
  def isZeroOrNull(col: ConstOrColMagnet[_]): IsZeroOrNull = IsZeroOrNull(col)

  def ifNull[V](col: ConstOrColMagnet[V], alternative: ConstOrColMagnet[_]): IfNull[V] = IfNull(col, alternative)
  def nullIf[V](col: ConstOrColMagnet[V], other: ConstOrColMagnet[_]): NullIf[V]       = NullIf(col, other)
  def assumeNotNull[V](col: ConstOrColMagnet[V]): AssumeNotNull[V]                     = AssumeNotNull(col)
  def toNullable[V](col: ConstOrColMagnet[V]): ToNullable[V]                           = ToNullable(col)
}
