package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait HigherOrderFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  private def tokenizeHOParams(col: HigherOrderFunction[_, _]): String = {

    val arrn = tokenizeSeqCol(col.arrn.map(_.column))
    fast"${tokenizeColumn(col.arr1.column)},$arrn"
    }

  def tokenizeHigherOrderFunction(col: HigherOrderFunction[_, _]): String = col match {
    case col: ArrayMap[_, _]         => fast"arrayMap(${tokenizeHOParams(col)})"
    case col: ArrayFilter[_, _]      => fast"arrayFilter(${tokenizeHOParams(col)})"
    case col: ArrayCount[_, _]       => fast"arrayCount(${tokenizeHOParams(col)})"
    case col: ArrayExists[_, _]      => fast"arrayExists(${tokenizeHOParams(col)})"
    case col: ArrayAll[_, _]         => fast"arrayAll(${tokenizeHOParams(col)})"
    case col: ArraySum[_, _]         => fast"arraySum(${tokenizeHOParams(col)})"
    case col: ArrayFirst[_, _]       => fast"arrayFirst(${tokenizeHOParams(col)})"
    case col: ArrayFirstIndex[_, _]  => fast"arrayFirstIndex(${tokenizeHOParams(col)})"
    case col: ArrayCumSum[_, _]      => fast"arrayCumSum(${tokenizeHOParams(col)})"
    case col: ArraySort[_, _]        => fast"arraySort(${tokenizeHOParams(col)})"
    case col: ArrayReverseSort[_, _] => fast"arrayReverseSort(${tokenizeHOParams(col)})"
  }

}
