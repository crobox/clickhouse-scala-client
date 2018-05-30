package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{ExpressionColumn, TableColumn}

trait HigherOrderFunctions { self: Magnets =>
  abstract class HigherOrderFunction[I,O](func: Option[(TableColumn[I] => ExpressionColumn[O])], arr1: ArrayColMagnet, arrn: ArrayColMagnet*)

  case class ArrayMap[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) extends HigherOrderFunction[I,O](Some(func), arr1, arrn:_*)
  case class ArrayFilter[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) extends HigherOrderFunction[I,O](Some(func), arr1, arrn:_*)
  case class ArrayCount[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) extends HigherOrderFunction[I,O](Some(func), arr1, arrn:_*)
  case class ArrayExists[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) extends HigherOrderFunction[I,O](Some(func), arr1, arrn:_*)
  case class ArrayAll[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) extends HigherOrderFunction[I,O](Some(func), arr1, arrn:_*)
  case class ArraySum[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) extends HigherOrderFunction[I,O](Some(func), arr1, arrn:_*)
  case class ArrayFirst[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) extends HigherOrderFunction[I,O](Some(func), arr1, arrn:_*)
  case class ArrayFirstIndex[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) extends HigherOrderFunction[I,O](Some(func), arr1, arrn:_*)
  case class ArrayCumSum[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) extends HigherOrderFunction[I,O](Some(func), arr1, arrn:_*)
  case class ArraySort[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) extends HigherOrderFunction[I,O](Some(func), arr1, arrn:_*)
  case class ArrayReverseSort[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) extends HigherOrderFunction[I,O](Some(func), arr1, arrn:_*)

  def arrayMap[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) = ArrayMap(func,arr1,arrn:_*)
  def arrayFilter[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) = ArrayMap(func,arr1,arrn:_*)
  def arrayCount[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) = ArrayMap(func,arr1,arrn:_*)
  def arrayExists[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) = ArrayMap(func,arr1,arrn:_*)
  def arrayAll[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) = ArrayMap(func,arr1,arrn:_*)
  def arraySum[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) = ArrayMap(func,arr1,arrn:_*)
  def arrayFirst[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) = ArrayMap(func,arr1,arrn:_*)
  def arrayFirstIndex[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) = ArrayMap(func,arr1,arrn:_*)
  def arrayCumSum[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) = ArrayMap(func,arr1,arrn:_*)
  def arraySort[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) = ArrayMap(func,arr1,arrn:_*)
  def arrayReverseSort[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet, arrn: ArrayColMagnet*) = ArrayMap(func,arr1,arrn:_*)

/*
arrayMap(func, arr1, ...)
arrayFilter(func, arr1, ...)
arrayCount([func,] arr1, ...)
arrayExists([func,] arr1, ...)
arrayAll([func,] arr1, ...)
arraySum([func,] arr1, ...)
arrayFirst(func, arr1, ...)
arrayFirstIndex(func, arr1, ...)
arrayCumSum([func,] arr1, ...)
arraySort([func,] arr1, ...)
arrayReverseSort([func,] arr1, ...)
 */

}
