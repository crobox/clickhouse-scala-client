package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn, TableColumn}

trait HigherOrderFunctions { self: Magnets =>
  abstract class HigherOrderFunction[I, O, R](val func: Option[TableColumn[I] => ExpressionColumn[O]],
                                              val arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends ExpressionColumn[R](EmptyColumn)

  case class ArrayMap[I, O](_func: TableColumn[I] => ExpressionColumn[O], _arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, O, Iterable[I]](Some(_func), _arr1)
  case class ArrayFilter[I](_func: TableColumn[I] => ExpressionColumn[Boolean], _arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, Boolean, Iterable[I]](Some(_func), _arr1)
  case class ArrayCount[I](_func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                           _arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, Boolean, Long](_func, _arr1)
  case class ArrayExists[I](_func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                            _arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, Boolean, Boolean](_func, _arr1)
  case class ArrayAll[I, O](_func: Option[TableColumn[I] => ExpressionColumn[O]],
                            _arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, O, Boolean](_func, _arr1)
  case class ArraySum[I, O](_func: Option[TableColumn[I] => ExpressionColumn[O]],
                            _arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, O, Long](_func, _arr1)
  case class ArrayFirst[I](_func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                           _arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, Boolean, I](_func, _arr1)
  case class ArrayFirstIndex[I](_func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                                _arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, Boolean, Long](_func, _arr1)
  case class ArrayCumSum[I, O](_func: Option[TableColumn[I] => ExpressionColumn[O]],
                               _arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, O, Iterable[Long]](_func, _arr1)
  case class ArraySort[I, O](_func: Option[TableColumn[I] => ExpressionColumn[O]],
                             _arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, O, Iterable[I]](_func, _arr1)
  case class ArrayReverseSort[I, O](_func: Option[TableColumn[I] => ExpressionColumn[O]],
                                    _arr1: ArrayColMagnet[_ <: Iterable[I]])
      extends HigherOrderFunction[I, O, Iterable[I]](_func, _arr1)

  def arrayMap[I, O](func: TableColumn[I] => ExpressionColumn[O],
                     arr1: ArrayColMagnet[_ <: Iterable[I]]): ArrayMap[I, O] =
    ArrayMap(func, arr1)

  def arrayFilter[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                     arr1: ArrayColMagnet[_ <: Iterable[I]]): ArrayFilter[I] =
    ArrayFilter(func, arr1)

  def arrayCount[I](func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                    arr1: ArrayColMagnet[_ <: Iterable[I]]): ArrayCount[I] =
    ArrayCount(func, arr1)

  def arrayExists[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                     arr1: ArrayColMagnet[_ <: Iterable[I]]): ArrayExists[I] =
    ArrayExists[I](Some(func), arr1)

  def arrayAll[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                  arr1: ArrayColMagnet[_ <: Iterable[I]]): ArrayAll[I, Boolean] =
    ArrayAll(Some(func), arr1)

  def arraySum[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                     arr1: ArrayColMagnet[_ <: Iterable[I]]): ArraySum[I, O] =
    ArraySum(func, arr1)

  def arrayFirst[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                    arr1: ArrayColMagnet[_ <: Iterable[I]]): ArrayFirst[I] =
    ArrayFirst(Some(func), arr1)

  def arrayFirstIndex[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                         arr1: ArrayColMagnet[_ <: Iterable[I]]): ArrayFirstIndex[I] =
    ArrayFirstIndex(Some(func), arr1)

  def arrayCumSum[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                        arr1: ArrayColMagnet[_ <: Iterable[I]]): ArrayCumSum[I, O] =
    ArrayCumSum(func, arr1)

  def arraySort[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                      arr1: ArrayColMagnet[_ <: Iterable[I]]): ArraySort[I, O] =
    ArraySort(func, arr1)

  def arrayReverseSort[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                             arr1: ArrayColMagnet[_ <: Iterable[I]]): ArrayReverseSort[I, O] =
    ArrayReverseSort(func, arr1)
}
