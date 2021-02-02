package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn, TableColumn}

trait HigherOrderFunctions { self: Magnets =>
  abstract class HigherOrderFunction[I, O, R](
      val func1: Option[TableColumn[I] => ExpressionColumn[O]],
      val func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
      val func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
      val arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends ExpressionColumn[R](EmptyColumn)

  // double type casts
  case class ArrayAll[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                            _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, Iterable[I]](_func1, _func2, _func3, _arrays: _*)
  case class ArrayCumSum[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                               _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                               _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                               _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, Iterable[I]](_func1, _func2, _func3, _arrays: _*)
  case class ArrayMap[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                            _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, Iterable[I]](_func1, _func2, _func3, _arrays: _*)
  case class ArrayReverseSort[I, O](
      _func1: Option[TableColumn[I] => ExpressionColumn[O]],
      _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
      _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, O, Iterable[I]](_func1, _func2, _func3, _arrays: _*)
  case class ArraySort[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                             _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                             _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                             _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, Iterable[I]](_func1, _func2, _func3, _arrays: _*)
  case class ArraySum[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                            _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, Iterable[I]](_func1, _func2, _func3, _arrays: _*)

  // single type casts
  case class ArrayCount[I](
      _func1: Option[TableColumn[I] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, Boolean, Iterable[I]](_func1, _func2, _func3, _arrays: _*)
  case class ArrayExists[I](
      _func1: Option[TableColumn[I] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, Boolean, Iterable[I]](_func1, _func2, _func3, _arrays: _*)
  case class ArrayFilter[I](
      _func1: Option[TableColumn[I] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, Boolean, Iterable[I]](_func1, _func2, _func3, _arrays: _*)
  case class ArrayFirst[I](
      _func1: Option[TableColumn[I] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, Boolean, Iterable[I]](_func1, _func2, _func3, _arrays: _*)
  case class ArrayFirstIndex[I](
      _func1: Option[TableColumn[I] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, Boolean, Iterable[I]](_func1, _func2, _func3, _arrays: _*)

  def arrayAll[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                  array: ArrayColMagnet[_ <: Iterable[I]]): ArrayAll[I, Boolean] =
    ArrayAll(Option(func), None, None, array)

  def arrayAll2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                   array1: ArrayColMagnet[_ <: Iterable[I]],
                   array2: ArrayColMagnet[_ <: Iterable[I]]): ArrayAll[I, Boolean] =
    ArrayAll(None, Option(func), None, array1, array2)

  def arrayAll3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                   array1: ArrayColMagnet[_ <: Iterable[I]],
                   array2: ArrayColMagnet[_ <: Iterable[I]],
                   array3: ArrayColMagnet[_ <: Iterable[I]]): ArrayAll[I, Boolean] =
    ArrayAll(None, None, Option(func), array1, array2, array3)

  def arrayCount[I](func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                    array: ArrayColMagnet[_ <: Iterable[I]]): ArrayCount[I] =
    ArrayCount(func, None, None, array)

  def arrayCount2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                     array1: ArrayColMagnet[_ <: Iterable[I]],
                     array2: ArrayColMagnet[_ <: Iterable[I]]): ArrayCount[I] =
    ArrayCount(None, Option(func), None, array1, array2)

  def arrayCount3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                     array1: ArrayColMagnet[_ <: Iterable[I]],
                     array2: ArrayColMagnet[_ <: Iterable[I]],
                     array3: ArrayColMagnet[_ <: Iterable[I]]): ArrayCount[I] =
    ArrayCount(None, None, Option(func), array1, array2, array3)

  def arrayExists[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ArrayExists[I] =
    ArrayExists(Option(func), None, None, array)

  def arrayExists2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]]): ArrayExists[I] =
    ArrayExists(None, Option(func), None, array1, array2)

  def arrayExists3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]],
                      array3: ArrayColMagnet[_ <: Iterable[I]]): ArrayExists[I] =
    ArrayExists(None, None, Option(func), array1, array2, array3)

  def arrayFilter[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ArrayFilter[I] =
    ArrayFilter(Option(func), None, None, array)

  def arrayFilter2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]]): ArrayFilter[I] =
    ArrayFilter(None, Option(func), None, array1, array2)

  def arrayFilter3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]],
                      array3: ArrayColMagnet[_ <: Iterable[I]]): ArrayFilter[I] =
    ArrayFilter(None, None, Option(func), array1, array2, array3)

  def arrayFirst[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                    array: ArrayColMagnet[_ <: Iterable[I]]): ArrayFirst[I] =
    ArrayFirst(Option(func), None, None, array)

  def arrayFirst2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                     array1: ArrayColMagnet[_ <: Iterable[I]],
                     array2: ArrayColMagnet[_ <: Iterable[I]]): ArrayFirst[I] =
    ArrayFirst(None, Option(func), None, array1, array2)

  def arrayFirst3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                     array1: ArrayColMagnet[_ <: Iterable[I]],
                     array2: ArrayColMagnet[_ <: Iterable[I]],
                     array3: ArrayColMagnet[_ <: Iterable[I]]): ArrayFirst[I] =
    ArrayFirst(None, None, Option(func), array1, array2, array3)

  def arrayFirstIndex[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                         array: ArrayColMagnet[_ <: Iterable[I]]): ArrayFirstIndex[I] =
    ArrayFirstIndex(Option(func), None, None, array)

  def arrayFirstIndex2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                          array1: ArrayColMagnet[_ <: Iterable[I]],
                          array2: ArrayColMagnet[_ <: Iterable[I]]): ArrayFirstIndex[I] =
    ArrayFirstIndex(None, Option(func), None, array1, array2)

  def arrayFirstIndex3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                          array1: ArrayColMagnet[_ <: Iterable[I]],
                          array2: ArrayColMagnet[_ <: Iterable[I]],
                          array3: ArrayColMagnet[_ <: Iterable[I]]): ArrayFirstIndex[I] =
    ArrayFirstIndex(None, None, Option(func), array1, array2, array3)

  def arrayMap[I, O](func: TableColumn[I] => ExpressionColumn[O],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ArrayMap[I, O] =
    ArrayMap(Option(func), None, None, array)

  def arrayMap2[I, O](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]]): ArrayMap[I, O] =
    ArrayMap(None, Option(func), None, array1, array2)

  def arrayMap3[I, O](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]],
                      array3: ArrayColMagnet[_ <: Iterable[I]]): ArrayMap[I, O] =
    ArrayMap(None, None, Option(func), array1, array2, array3)

  def arraySum[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ArraySum[I, O] =
    ArraySum(func, None, None, array)

  def arrayCumSum[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                        array: ArrayColMagnet[_ <: Iterable[I]]): ArrayCumSum[I, O] =
    ArrayCumSum(func, None, None, array)

  def arraySort[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                      array: ArrayColMagnet[_ <: Iterable[I]]): ArraySort[I, O] =
    ArraySort(func, None, None, array)

  def arrayReverseSort[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                             array: ArrayColMagnet[_ <: Iterable[I]]): ArrayReverseSort[I, O] =
    ArrayReverseSort(func, None, None, array)
}
