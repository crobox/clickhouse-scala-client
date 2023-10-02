package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Column, ExpressionColumn}

trait SplitMergeFunctions { self: Magnets =>

  abstract class SplitMergeFunction[V](col: Column) extends ExpressionColumn[V](col)

  case class SplitByChar(sep: StringColMagnet[_], col: StringColMagnet[_])
      extends SplitMergeFunction[Iterable[String]](col.column)
  case class SplitByString(sep: StringColMagnet[_], col: StringColMagnet[_])
      extends SplitMergeFunction[Iterable[String]](col.column)
  case class ArrayStringConcat(col: ArrayColMagnet[_], sep: StringColMagnet[_])
      extends SplitMergeFunction[String](col.column)
  case class AlphaTokens(col: StringColMagnet[_]) extends SplitMergeFunction[Iterable[String]](col.column)

  def splitByChar(sep: StringColMagnet[_], col: StringColMagnet[_]): SplitByChar     = SplitByChar(sep, col)
  def splitByString(sep: StringColMagnet[_], col: StringColMagnet[_]): SplitByString = SplitByString(sep, col)

  def arrayStringConcat(col: ArrayColMagnet[_], sep: StringColMagnet[_] = ""): ArrayStringConcat =
    ArrayStringConcat(col, sep)
  def alphaTokens(col: StringColMagnet[_]): AlphaTokens = AlphaTokens(col)
}
