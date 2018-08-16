package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

trait SplitMergeFunctions { self: Magnets =>

  abstract class SplitMergeFunction[V](col: AnyTableColumn) extends ExpressionColumn[V](col)

  case class SplitByChar(sep: StringColMagnet[_], col: StringColMagnet[_]) extends SplitMergeFunction[Iterable[String]](col.column)
  case class SplitByString(sep: StringColMagnet[_], col: StringColMagnet[_]) extends SplitMergeFunction[Iterable[String]](col.column)
  case class ArrayStringConcat(col: ArrayColMagnet[_], sep: StringColMagnet[_]) extends SplitMergeFunction[String](col.column)
  case class AlphaTokens(col: StringColMagnet[_]) extends SplitMergeFunction[Iterable[String]](col.column)

  def splitByChar(sep: StringColMagnet[_], col: StringColMagnet[_]) = SplitByChar(sep, col)
  def splitByString(sep: StringColMagnet[_], col: StringColMagnet[_]) = SplitByString(sep, col)
  def arrayStringConcat(col: ArrayColMagnet[_], sep: StringColMagnet[_] = "") = ArrayStringConcat(col,sep)
  def alphaTokens(col: StringColMagnet[_]) = AlphaTokens(col)
  /*
splitByChar(separator, s)
splitByString(separator, s)
arrayStringConcat(arr[, separator])
alphaTokens(s)
 */
}
