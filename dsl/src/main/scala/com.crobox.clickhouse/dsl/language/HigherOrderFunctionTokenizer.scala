package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.dongxiguo.fastring.Fastring.Implicits._

trait HigherOrderFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  private def tokenizeHOFunc[I,O](func: TableColumn[I] => ExpressionColumn[O] )(implicit database: Database): String = {
    val in: TableColumn[I] = RefColumn[I]("x")
    "x -> " + tokenizeColumn(func(in))
  }

  private def tokenizeHOParams[I,O,R](col: HigherOrderFunction[I, O, R])(implicit database: Database): String = {
    val funcPart = col.func.map(col=> tokenizeHOFunc[I,O](col) + ", ").getOrElse("")
    funcPart + tokenizeColumn(col.arr1.column)
  }

  def tokenizeHigherOrderFunction(col: HigherOrderFunction[_, _, _])(implicit database: Database): String = col match {
    case col: ArrayMap[_, _]         => fast"arrayMap(${tokenizeHOParams(col)})"
    case col: ArrayFilter[_]         => fast"arrayFilter(${tokenizeHOParams(col)})"
    case col: ArrayCount[_]          => fast"arrayCount(${tokenizeHOParams(col)})"
    case col: ArrayExists[_]         => fast"arrayExists(${tokenizeHOParams(col)})"
    case col: ArrayAll[_, _]         => fast"arrayAll(${tokenizeHOParams(col)})"
    case col: ArraySum[_, _]         => fast"arraySum(${tokenizeHOParams(col)})"
    case col: ArrayFirst[_]          => fast"arrayFirst(${tokenizeHOParams(col)})"
    case col: ArrayFirstIndex[_]     => fast"arrayFirstIndex(${tokenizeHOParams(col)})"
    case col: ArrayCumSum[_, _]      => fast"arrayCumSum(${tokenizeHOParams(col)})"
    case col: ArraySort[_, _]        => fast"arraySort(${tokenizeHOParams(col)})"
    case col: ArrayReverseSort[_, _] => fast"arrayReverseSort(${tokenizeHOParams(col)})"
  }

}
