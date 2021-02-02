package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.{ExpressionColumn, TableColumn, _}

trait HigherOrderFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  private def tokenizeHOFunc[I, O, R](col: HigherOrderFunction[I, O, R])(implicit ctx: TokenizeContext): String =
    if (col.func1.isDefined) {
      "x -> " + tokenizeColumn(col.func1.get(RefColumn[I]("x"))) + ","
    } else if (col.func2.isDefined) {
      "(x,y) -> " + tokenizeColumn(col.func2.get(RefColumn[I]("x"), RefColumn[I]("y"))) + ","
    } else if (col.func3.isDefined) {
      "(x,y,z) -> " + tokenizeColumn(col.func3.get(RefColumn[I]("x"), RefColumn[I]("y"), RefColumn[I]("z"))) + ","
    } else ""

  private def tokenizeHOParams[I, O, R](col: HigherOrderFunction[I, O, R])(implicit ctx: TokenizeContext): String =
    tokenizeHOFunc(col) + tokenizeColumns(col.arrays.map(_.column))

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
