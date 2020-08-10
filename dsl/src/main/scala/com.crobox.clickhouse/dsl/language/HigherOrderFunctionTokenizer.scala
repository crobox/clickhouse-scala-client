package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait HigherOrderFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  private def tokenizeHOFunc[I, O](
      func: TableColumn[I] => ExpressionColumn[O]
  )(implicit ctx: TokenizeContext): String = {
    val in: TableColumn[I] = RefColumn[I]("x")
    "x -> " + tokenizeColumn(func(in))
  }

  private def tokenizeHOParams[I, O, R](col: HigherOrderFunction[I, O, R])(implicit ctx: TokenizeContext): String = {
    val funcPart = col.func.map(col => tokenizeHOFunc[I, O](col) + ", ").getOrElse("")
    funcPart + tokenizeColumn(col.arr1.column)
  }

  def tokenizeHigherOrderFunction(col: HigherOrderFunction[_, _, _])(implicit ctx: TokenizeContext): String =
    col match {
      case col: ArrayMap[_, _]         => s"arrayMap(${tokenizeHOParams(col)})"
      case col: ArrayFilter[_]         => s"arrayFilter(${tokenizeHOParams(col)})"
      case col: ArrayCount[_]          => s"arrayCount(${tokenizeHOParams(col)})"
      case col: ArrayExists[_]         => s"arrayExists(${tokenizeHOParams(col)})"
      case col: ArrayAll[_, _]         => s"arrayAll(${tokenizeHOParams(col)})"
      case col: ArraySum[_, _]         => s"arraySum(${tokenizeHOParams(col)})"
      case col: ArrayFirst[_]          => s"arrayFirst(${tokenizeHOParams(col)})"
      case col: ArrayFirstIndex[_]     => s"arrayFirstIndex(${tokenizeHOParams(col)})"
      case col: ArrayCumSum[_, _]      => s"arrayCumSum(${tokenizeHOParams(col)})"
      case col: ArraySort[_, _]        => s"arraySort(${tokenizeHOParams(col)})"
      case col: ArrayReverseSort[_, _] => s"arrayReverseSort(${tokenizeHOParams(col)})"
    }

}
