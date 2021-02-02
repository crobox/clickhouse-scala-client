package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn, TableColumn}

trait HigherOrderFunctions { self: Magnets =>
  abstract class HigherOrderFunction[I, O, R](
      val func1: Option[TableColumn[I] => ExpressionColumn[O]],
      val func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
      val arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends ExpressionColumn[R](EmptyColumn)

  case class ArrayMap[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                            _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, Iterable[I]](_func1, _func2, _arrays: _*)
  case class ArrayFilter[I](_func: TableColumn[I] => ExpressionColumn[Boolean],
                            _array: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, Boolean, Iterable[I]](Some(_func), None, _array)
  case class ArrayCount[I](_func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                           _array: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, Boolean, Long](_func, None, _array)
  case class ArrayExists[I](_func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                            _array: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, Boolean, Boolean](_func, None, _array)
  case class ArrayAll[I, O](_func: Option[TableColumn[I] => ExpressionColumn[O]],
                            _array: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, O, Boolean](_func, None, _array)
  case class ArraySum[I, O](_func: Option[TableColumn[I] => ExpressionColumn[O]],
                            _array: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, O, Long](_func, None, _array)
  case class ArrayFirst[I](_func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                           _array: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, Boolean, I](_func, None, _array)
  case class ArrayFirstIndex[I](_func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                                _array: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, Boolean, Long](_func, None, _array)
  case class ArrayCumSum[I, O](_func: Option[TableColumn[I] => ExpressionColumn[O]],
                               _array: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, O, Iterable[Long]](_func, None, _array)
  case class ArraySort[I, O](_func: Option[TableColumn[I] => ExpressionColumn[O]],
                             _array: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, O, Iterable[I]](_func, None, _array)
  case class ArrayReverseSort[I, O](_func: Option[TableColumn[I] => ExpressionColumn[O]],
                                    _array: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, O, Iterable[I]](_func, None, _array)

  def arrayMap[I, O](func: TableColumn[I] => ExpressionColumn[O],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ArrayMap[I, O] =
    ArrayMap(Option(func), None, array)

  def arrayMap[I, O](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                     array1: ArrayColMagnet[_ <: Iterable[I]],
                     array2: ArrayColMagnet[_ <: Iterable[I]]): ArrayMap[I, O] =
    ArrayMap(None, Option(func), array1, array2)

  def arrayFilter[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ArrayFilter[I] =
    ArrayFilter(func, array)

  def arrayCount[I](func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                    array: ArrayColMagnet[_ <: Iterable[I]]): ArrayCount[I] =
    ArrayCount(func, array)

  def arrayExists[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ArrayExists[I] =
    ArrayExists[I](Some(func), array)

  def arrayAll[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                  array: ArrayColMagnet[_ <: Iterable[I]]): ArrayAll[I, Boolean] =
    ArrayAll(Some(func), array)

  def arraySum[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ArraySum[I, O] =
    ArraySum(func, array)

  def arrayFirst[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                    array: ArrayColMagnet[_ <: Iterable[I]]): ArrayFirst[I] =
    ArrayFirst(Some(func), array)

  def arrayFirstIndex[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                         array: ArrayColMagnet[_ <: Iterable[I]]): ArrayFirstIndex[I] =
    ArrayFirstIndex(Some(func), array)

  def arrayCumSum[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                        array: ArrayColMagnet[_ <: Iterable[I]]): ArrayCumSum[I, O] =
    ArrayCumSum(func, array)

  def arraySort[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                      array: ArrayColMagnet[_ <: Iterable[I]]): ArraySort[I, O] =
    ArraySort(func, array)

  def arrayReverseSort[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                             array: ArrayColMagnet[_ <: Iterable[I]]): ArrayReverseSort[I, O] =
    ArrayReverseSort(func, array)
}
