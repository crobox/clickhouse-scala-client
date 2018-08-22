package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn, TableColumn}

trait HigherOrderFunctions { self: Magnets =>
  abstract class HigherOrderFunction[I,O,R](val func: Option[(TableColumn[I] => ExpressionColumn[O])], val arr1: ArrayColMagnet[Iterable[I]], val arrn: ArrayColMagnet[Iterable[I]]*) extends ExpressionColumn[R](EmptyColumn())
//TODO funcs should be optional mostly
  case class ArrayMap[I,O]   (_func: (TableColumn[I] => ExpressionColumn[O]), _arr1: ArrayColMagnet[Iterable[I]], _arrn: ArrayColMagnet[Iterable[I]]*)      extends HigherOrderFunction[I,O,Iterable[I]](Some(_func), _arr1, _arrn:_*)
  case class ArrayFilter[I](_func: (TableColumn[I] => ExpressionColumn[Boolean]), _arr1: ArrayColMagnet[Iterable[I]], _arrn: ArrayColMagnet[Iterable[I]]*)      extends HigherOrderFunction[I,Boolean,Iterable[I]](Some(_func), _arr1, _arrn:_*)
  case class ArrayCount[I,O] (_func: Option[(TableColumn[I] => ExpressionColumn[O])], _arr1: ArrayColMagnet[Iterable[I]], _arrn: ArrayColMagnet[Iterable[I]]*)      extends HigherOrderFunction[I,O,Long](Some(_func), _arr1, _arrn:_*)
  case class ArrayExists[I,O](_func: Option[(TableColumn[I] => ExpressionColumn[O])], _arr1: ArrayColMagnet[Iterable[I]], _arrn: ArrayColMagnet[Iterable[I]]*)      extends HigherOrderFunction[I,O,Boolean](Some(_func), _arr1, _arrn:_*)
  case class ArrayAll[I,O]   (_func: Option[(TableColumn[I] => ExpressionColumn[O])], _arr1: ArrayColMagnet[Iterable[I]], _arrn: ArrayColMagnet[Iterable[I]]*)      extends HigherOrderFunction[I,O,Boolean](Some(_func), _arr1, _arrn:_*)
  case class ArraySum[I,O]   (_func: Option[(TableColumn[I] => ExpressionColumn[O])], _arr1: ArrayColMagnet[Iterable[I]], _arrn: ArrayColMagnet[Iterable[I]]*)      extends HigherOrderFunction[I,O,Long](Some(_func), _arr1, _arrn:_*)
  case class ArrayFirst[I,O] (_func: Option[(TableColumn[I] => ExpressionColumn[O])], _arr1: ArrayColMagnet[Iterable[I]], _arrn: ArrayColMagnet[Iterable[I]]*)      extends HigherOrderFunction[I,O,I](Some(_func), _arr1, _arrn:_*)
  case class ArrayFirstIndex[I,O](_func: Option[(TableColumn[I] => ExpressionColumn[O])], _arr1: ArrayColMagnet[Iterable[I]], _arrn: ArrayColMagnet[Iterable[I]]*)  extends HigherOrderFunction[I,O,Long](Some(_func), _arr1, _arrn:_*)
  case class ArrayCumSum[I,O](_func: Option[(TableColumn[I] => ExpressionColumn[O])], _arr1: ArrayColMagnet[Iterable[I]], _arrn: ArrayColMagnet[Iterable[I]]*)      extends HigherOrderFunction[I,O,Iterable[Long]](Some(_func), _arr1, _arrn:_*)
  case class ArraySort[I,O]  (_func: Option[(TableColumn[I] => ExpressionColumn[O])], _arr1: ArrayColMagnet[Iterable[I]], _arrn: ArrayColMagnet[Iterable[I]]*)      extends HigherOrderFunction[I,O,Iterable[I]](Some(_func), _arr1, _arrn:_*)
  case class ArrayReverseSort[I,O](_func: Option[(TableColumn[I] => ExpressionColumn[O])], _arr1: ArrayColMagnet[Iterable[I]], _arrn: ArrayColMagnet[Iterable[I]]*) extends HigherOrderFunction[I,O,Iterable[I]](Some(_func), _arr1, _arrn:_*)

  def arrayMap[I,O]   (func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet[Iterable[I]], arrn: ArrayColMagnet[Iterable[I]]*) = ArrayMap(func,arr1,arrn:_*)
  def arrayFilter[I](func: (TableColumn[I] => ExpressionColumn[Boolean]), arr1: ArrayColMagnet[Iterable[I]], arrn: ArrayColMagnet[Iterable[I]]*) = ArrayFilter(func,arr1,arrn:_*)
  def arrayCount[I,O] (func: Option[(TableColumn[I] => ExpressionColumn[O])], arr1: ArrayColMagnet[Iterable[I]], arrn: ArrayColMagnet[Iterable[I]]*) = ArrayCount(func,arr1,arrn:_*)
  def arrayExists[I,O](func: Option[(TableColumn[I] => ExpressionColumn[O])], arr1: ArrayColMagnet[Iterable[I]], arrn: ArrayColMagnet[Iterable[I]]*) = ArrayExists(func,arr1,arrn:_*)
  def arrayAll[I,O]   (func: Option[(TableColumn[I] => ExpressionColumn[O])], arr1: ArrayColMagnet[Iterable[I]], arrn: ArrayColMagnet[Iterable[I]]*) = ArrayAll(func,arr1,arrn:_*)
  def arraySum[I,O]   (func: Option[(TableColumn[I] => ExpressionColumn[O])], arr1: ArrayColMagnet[Iterable[I]], arrn: ArrayColMagnet[Iterable[I]]*) = ArraySum(func,arr1,arrn:_*)
  def arrayFirst[I,O] (func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet[Iterable[I]], arrn: ArrayColMagnet[Iterable[I]]*) = ArrayFirst(func,arr1,arrn:_*)
  def arrayFirstIndex[I,O](func: (TableColumn[I] => ExpressionColumn[O]), arr1: ArrayColMagnet[Iterable[I]], arrn: ArrayColMagnet[Iterable[I]]*) = ArrayFirstIndex(func,arr1,arrn:_*)
  def arrayCumSum[I,O](func: Option[(TableColumn[I] => ExpressionColumn[O])], arr1: ArrayColMagnet[Iterable[I]], arrn: ArrayColMagnet[Iterable[I]]*) = ArrayCumSum(func,arr1,arrn:_*)
  def arraySort[I,O]  (func: Option[(TableColumn[I] => ExpressionColumn[O])], arr1: ArrayColMagnet[Iterable[I]], arrn: ArrayColMagnet[Iterable[I]]*) = ArraySort(func,arr1,arrn:_*)
  def arrayReverseSort[I,O](func: Option[(TableColumn[I] => ExpressionColumn[O])], arr1: ArrayColMagnet[Iterable[I]], arrn: ArrayColMagnet[Iterable[I]]*) = ArrayReverseSort(func,arr1,arrn:_*)

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
