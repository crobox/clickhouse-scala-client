package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn, TableColumn}

trait HigherOrderFunctions { self: Magnets =>
  abstract class HigherOrderFunction[I,O,R](val func: Option[TableColumn[I] => ExpressionColumn[O]], val arr1: ArrayColMagnet[Iterable[I]]) extends ExpressionColumn[R](EmptyColumn())
//TODO funcs should be optional mostly
  case class ArrayMap[I,O]   (_func: TableColumn[I] => ExpressionColumn[O], _arr1: ArrayColMagnet[Iterable[I]])      extends HigherOrderFunction[I,O,Iterable[I]](Some(_func), _arr1)
  case class ArrayFilter[I](_func: TableColumn[I] => ExpressionColumn[Boolean] , _arr1: ArrayColMagnet[Iterable[I]])      extends HigherOrderFunction[I,Boolean,Iterable[I]](Some(_func), _arr1)
  case class ArrayCount[I] (_func: Option[TableColumn[I] => ExpressionColumn[Boolean]], _arr1: ArrayColMagnet[Iterable[I]])      extends HigherOrderFunction[I,Boolean,Long](_func, _arr1)
  case class ArrayExists[I](_func: Option[TableColumn[I] => ExpressionColumn[Boolean]], _arr1: ArrayColMagnet[Iterable[I]])      extends HigherOrderFunction[I,Boolean,Boolean](_func, _arr1)
  case class ArrayAll[I,O]   (_func: Option[TableColumn[I] => ExpressionColumn[O]], _arr1: ArrayColMagnet[Iterable[I]])      extends HigherOrderFunction[I,O,Boolean](_func, _arr1)
  case class ArraySum[I,O]   (_func: Option[TableColumn[I] => ExpressionColumn[O]], _arr1: ArrayColMagnet[Iterable[I]])      extends HigherOrderFunction[I,O,Long](_func, _arr1)
  case class ArrayFirst[I] (_func: Option[TableColumn[I] => ExpressionColumn[Boolean]], _arr1: ArrayColMagnet[Iterable[I]])      extends HigherOrderFunction[I,Boolean,I](_func, _arr1)
  case class ArrayFirstIndex[I](_func: Option[TableColumn[I] => ExpressionColumn[Boolean]], _arr1: ArrayColMagnet[Iterable[I]])  extends HigherOrderFunction[I,Boolean,Long](_func, _arr1)
  case class ArrayCumSum[I,O](_func: Option[TableColumn[I] => ExpressionColumn[O]], _arr1: ArrayColMagnet[Iterable[I]])      extends HigherOrderFunction[I,O,Iterable[Long]](_func, _arr1)
  case class ArraySort[I,O]  (_func: Option[TableColumn[I] => ExpressionColumn[O]], _arr1: ArrayColMagnet[Iterable[I]])      extends HigherOrderFunction[I,O,Iterable[I]](_func, _arr1)
  case class ArrayReverseSort[I,O](_func: Option[TableColumn[I] => ExpressionColumn[O]], _arr1: ArrayColMagnet[Iterable[I]]) extends HigherOrderFunction[I,O,Iterable[I]](_func, _arr1)

  def arrayMap[I,O]   (func: TableColumn[I] => ExpressionColumn[O], arr1: ArrayColMagnet[Iterable[I]]) = ArrayMap(func,arr1)
  def arrayFilter[I](func: TableColumn[I] => ExpressionColumn[Boolean] , arr1: ArrayColMagnet[Iterable[I]]) = ArrayFilter(func,arr1)
  def arrayCount[I] (func: Option[TableColumn[I] => ExpressionColumn[Boolean]], arr1: ArrayColMagnet[Iterable[I]]) = ArrayCount(func,arr1)
  def arrayExists[I](func: TableColumn[I] => ExpressionColumn[Boolean], arr1: ArrayColMagnet[Iterable[I]]) = ArrayExists[I](Some(func),arr1)
  def arrayAll[I]   (func: TableColumn[I] => ExpressionColumn[Boolean], arr1: ArrayColMagnet[Iterable[I]]) = ArrayAll(Some(func),arr1)
  def arraySum[I,O]   (func: Option[TableColumn[I] => ExpressionColumn[O]], arr1: ArrayColMagnet[Iterable[I]]) = ArraySum(func,arr1)

  def arrayFirst[I] (func: TableColumn[I] => ExpressionColumn[Boolean], arr1: ArrayColMagnet[Iterable[I]]) = ArrayFirst(Some(func),arr1)
  def arrayFirstIndex[I](func: TableColumn[I] => ExpressionColumn[Boolean], arr1: ArrayColMagnet[Iterable[I]]) = ArrayFirstIndex(Some(func),arr1)
  def arrayCumSum[I,O](func: Option[TableColumn[I] => ExpressionColumn[O]], arr1: ArrayColMagnet[Iterable[I]]) = ArrayCumSum(func,arr1)

  def arraySort[I,O]  (func: Option[TableColumn[I] => ExpressionColumn[O]], arr1: ArrayColMagnet[Iterable[I]]) = ArraySort(func,arr1)

  def arrayReverseSort[I,O](func: Option[TableColumn[I] => ExpressionColumn[O]], arr1: ArrayColMagnet[Iterable[I]]) = ArrayReverseSort(func,arr1)

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
