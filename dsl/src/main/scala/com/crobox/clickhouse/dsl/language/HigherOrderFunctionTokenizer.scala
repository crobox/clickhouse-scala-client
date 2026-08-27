package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.{ExpressionColumn, TableColumn, _}

trait HigherOrderFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  // Generic in the element types so each lambda parameter can be bound at its own array's type. The types are phantom
  // here -- a RefColumn only carries a name -- but keeping them lets the existentials in the dispatch below resolve
  // without a cast.
  private def tokenizeHOFunc[I1, I2, I3, O, R](
      col: HigherOrderFunction[I1, I2, I3, O, R]
  )(implicit ctx: TokenizeContext): String =
    if (col.func1.isDefined) {
      "x -> " + tokenizeColumn(col.func1.get(RefColumn[I1]("x"))) + ", "
    } else if (col.func2.isDefined) {
      "(x,y) -> " + tokenizeColumn(col.func2.get(RefColumn[I1]("x"), RefColumn[I2]("y"))) + ", "
    } else if (col.func3.isDefined) {
      "(x,y,z) -> " + tokenizeColumn(col.func3.get(RefColumn[I1]("x"), RefColumn[I2]("y"), RefColumn[I3]("z"))) + ", "
    } else ""

  private def tokenizeHOParams[I1, I2, I3, O, R](
      col: HigherOrderFunction[I1, I2, I3, O, R]
  )(implicit ctx: TokenizeContext): String =
    tokenizeHOFunc(col) + tokenizeColumns(col.arrays.map(_.column))

  def tokenizeHigherOrderFunction(col: HigherOrderFunction[_, _, _, _, _])(implicit ctx: TokenizeContext): String =
    col match {
      case col: ArrayAll[_, _, _]            => s"arrayAll(${tokenizeHOParams(col)})"
      case col: ArrayAvg[_, _, _, _]         => s"arrayAvg(${tokenizeHOParams(col)})"
      case col: ArrayCount[_, _, _]          => s"arrayCount(${tokenizeHOParams(col)})"
      case col: ArrayCumSum[_, _, _, _]      => s"arrayCumSum(${tokenizeHOParams(col)})"
      case col: ArrayExists[_, _, _]         => s"arrayExists(${tokenizeHOParams(col)})"
      case col: ArrayFill[_, _, _]           => s"arrayFill(${tokenizeHOParams(col)})"
      case col: ArrayFilter[_, _, _]         => s"arrayFilter(${tokenizeHOParams(col)})"
      case col: ArrayFirst[_, _, _]          => s"arrayFirst(${tokenizeHOParams(col)})"
      case col: ArrayFirstIndex[_, _, _]     => s"arrayFirstIndex(${tokenizeHOParams(col)})"
      case col: ArrayMap[_, _, _, _]         => s"arrayMap(${tokenizeHOParams(col)})"
      case col: ArrayMax[_, _, _, _]         => s"arrayMax(${tokenizeHOParams(col)})"
      case col: ArrayMin[_, _, _, _]         => s"arrayMin(${tokenizeHOParams(col)})"
      case col: ArrayReverseFill[_, _, _]    => s"arrayReverseFill(${tokenizeHOParams(col)})"
      case col: ArrayReverseSort[_, _, _, _] => s"arrayReverseSort(${tokenizeHOParams(col)})"
      case col: ArrayReverseSplit[_, _, _]   => s"arrayReverseSplit(${tokenizeHOParams(col)})"
      case col: ArraySort[_, _, _, _]        => s"arraySort(${tokenizeHOParams(col)})"
      case col: ArraySplit[_, _, _]          => s"arraySplit(${tokenizeHOParams(col)})"
      case col: ArraySum[_, _, _, _]         => s"arraySum(${tokenizeHOParams(col)})"
    }
}
