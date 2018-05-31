package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

trait SplitMergeFunctions { self: Magnets =>

  abstract class SplitMergeFunction[V](col: AnyTableColumn) extends ExpressionColumn[V](col)

  case class SplitByChar(sep: StringColMagnet, col: ArrayColMagnet) extends SplitMergeFunction[Iterable[String]](col.column)
  case class SplitByString(sep: StringColMagnet, col: ArrayColMagnet) extends SplitMergeFunction[Iterable[String]](col.column)
  case class ArrayStringConcat(col: StringColMagnet, sep: StringColMagnet = "") extends SplitMergeFunction[String](col.column)
  case class AlphaTokens(col: StringColMagnet) extends SplitMergeFunction[Iterable[String]](col.column)

  def splitByChar(sep: StringColMagnet, col: ArrayColMagnet) = SplitByChar(sep, col)
  def splitByString(sep: StringColMagnet, col: ArrayColMagnet) = SplitByString(sep, col)
  def arrayStringConcat(col: StringColMagnet) = ArrayStringConcat(col)
  def alphaTokens(col: StringColMagnet) = AlphaTokens(col)
  /*
splitByChar(separator, s)
splitByString(separator, s)
arrayStringConcat(arr[, separator])
alphaTokens(s)
 */
}
