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
      extends HigherOrderFunction[I, O, Boolean](_func1, _func2, _func3, _arrays: _*)
  case class ArrayAvg[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                            _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, Double](_func1, _func2, _func3, _arrays: _*)
  case class ArrayCumSum[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                               _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                               _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                               _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, Iterable[O]](_func1, _func2, _func3, _arrays: _*)
  case class ArrayMap[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                            _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, Iterable[O]](_func1, _func2, _func3, _arrays: _*)
  case class ArrayMax[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                            _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, O](_func1, _func2, _func3, _arrays: _*)
  case class ArrayMin[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                            _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, O](_func1, _func2, _func3, _arrays: _*)
  case class ArrayReverseSort[I, O](
      _func1: Option[TableColumn[I] => ExpressionColumn[O]],
      _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
      _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, O, Iterable[O]](_func1, _func2, _func3, _arrays: _*)
  case class ArraySort[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                             _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                             _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                             _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, Iterable[O]](_func1, _func2, _func3, _arrays: _*)
  case class ArraySum[I, O](_func1: Option[TableColumn[I] => ExpressionColumn[O]],
                            _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O]],
                            _arrays: ArrayColMagnet[_ <: Iterable[I]]*)
      extends HigherOrderFunction[I, O, O](_func1, _func2, _func3, _arrays: _*)

  // single type casts
  case class ArrayCount[I](
      _func1: Option[TableColumn[I] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, Boolean, Int](_func1, _func2, _func3, _arrays: _*)
  case class ArrayExists[I](
      _func1: Option[TableColumn[I] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, Boolean, Boolean](_func1, _func2, _func3, _arrays: _*)
  case class ArrayFill[I](
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
  ) extends HigherOrderFunction[I, Boolean, I](_func1, _func2, _func3, _arrays: _*)
  case class ArrayFirstIndex[I](
      _func1: Option[TableColumn[I] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, Boolean, Int](_func1, _func2, _func3, _arrays: _*)
  case class ArrayReverseFill[I](
      _func1: Option[TableColumn[I] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, Boolean, Iterable[I]](_func1, _func2, _func3, _arrays: _*)
  case class ArrayReverseSplit[I](
      _func2: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, Boolean, Iterable[Iterable[I]]](None, Option(_func2), None, _arrays: _*)
  case class ArraySplit[I](
      _func2: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
      _arrays: ArrayColMagnet[_ <: Iterable[I]]*
  ) extends HigherOrderFunction[I, Boolean, Iterable[Iterable[I]]](None, Option(_func2), None, _arrays: _*)

  def arrayAll[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                  array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Boolean] =
    ArrayAll(Option(func), None, None, array)

  def arrayAll2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                   array1: ArrayColMagnet[_ <: Iterable[I]],
                   array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Boolean] =
    ArrayAll(None, Option(func), None, array1, array2)

  def arrayAll3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                   array1: ArrayColMagnet[_ <: Iterable[I]],
                   array2: ArrayColMagnet[_ <: Iterable[I]],
                   array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Boolean] =
    ArrayAll(None, None, Option(func), array1, array2, array3)

  def arrayAvg[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Double] =
    ArrayAvg(func, None, None, array)

  def arrayAvg2[I, O](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Double] =
    ArrayAvg(None, Option(func), None, array1, array2)

  def arrayAvg3[I, O](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]],
                      array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Double] =
    ArrayAvg(None, None, Option(func), array1, array2, array3)

  def arrayCount[I](func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
                    array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Int] =
    ArrayCount(func, None, None, array)

  def arrayCount2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                     array1: ArrayColMagnet[_ <: Iterable[I]],
                     array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Int] =
    ArrayCount(None, Option(func), None, array1, array2)

  def arrayCount3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                     array1: ArrayColMagnet[_ <: Iterable[I]],
                     array2: ArrayColMagnet[_ <: Iterable[I]],
                     array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Int] =
    ArrayCount(None, None, Option(func), array1, array2, array3)

  def arrayCumSum[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                        array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArrayCumSum(func, None, None, array)

  def arrayCumSum2[I, O](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                         array1: ArrayColMagnet[_ <: Iterable[I]],
                         array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArrayCumSum(None, Option(func), None, array1, array2)

  def arrayCumSum3[I, O](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                         array1: ArrayColMagnet[_ <: Iterable[I]],
                         array2: ArrayColMagnet[_ <: Iterable[I]],
                         array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArrayCumSum(None, None, Option(func), array1, array2, array3)

  def arrayExists[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Boolean] =
    ArrayExists(Option(func), None, None, array)

  def arrayExists2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Boolean] =
    ArrayExists(None, Option(func), None, array1, array2)

  def arrayExists3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]],
                      array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Boolean] =
    ArrayExists(None, None, Option(func), array1, array2, array3)

  def arrayFill[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                   array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[I]] =
    ArrayFill(Option(func), None, None, array)

  // @todo This doesn't make sense
  def arrayFill2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                    array1: ArrayColMagnet[_ <: Iterable[I]],
                    array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[I]] =
    ArrayFill(None, Option(func), None, array1, array2)

  // @todo This doesn't  make sense
  def arrayFill3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                    array1: ArrayColMagnet[_ <: Iterable[I]],
                    array2: ArrayColMagnet[_ <: Iterable[I]],
                    array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[I]] =
    ArrayFill(None, None, Option(func), array1, array2, array3)

  def arrayFilter[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[I]] =
    ArrayFilter(Option(func), None, None, array)

  def arrayFilter2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[I]] =
    ArrayFilter(None, Option(func), None, array1, array2)

  def arrayFilter3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]],
                      array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[I]] =
    ArrayFilter(None, None, Option(func), array1, array2, array3)

  def arrayFirst[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                    array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[I] =
    ArrayFirst(Option(func), None, None, array)

  def arrayFirst2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                     array1: ArrayColMagnet[_ <: Iterable[I]],
                     array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[I] =
    ArrayFirst(None, Option(func), None, array1, array2)

  def arrayFirst3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                     array1: ArrayColMagnet[_ <: Iterable[I]],
                     array2: ArrayColMagnet[_ <: Iterable[I]],
                     array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[I] =
    ArrayFirst(None, None, Option(func), array1, array2, array3)

  def arrayFirstIndex[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                         array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Int] =
    ArrayFirstIndex(Option(func), None, None, array)

  def arrayFirstIndex2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                          array1: ArrayColMagnet[_ <: Iterable[I]],
                          array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Int] =
    ArrayFirstIndex(None, Option(func), None, array1, array2)

  def arrayFirstIndex3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                          array1: ArrayColMagnet[_ <: Iterable[I]],
                          array2: ArrayColMagnet[_ <: Iterable[I]],
                          array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Int] =
    ArrayFirstIndex(None, None, Option(func), array1, array2, array3)

  def arrayMap[I, O](func: TableColumn[I] => ExpressionColumn[O],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArrayMap(Option(func), None, None, array)

  def arrayMap2[I, O](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArrayMap(None, Option(func), None, array1, array2)

  def arrayMap3[I, O](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]],
                      array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArrayMap(None, None, Option(func), array1, array2, array3)

  def arrayMax[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[O] =
    ArrayMax(func, None, None, array)

  def arrayMax2[I, O](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[O] =
    ArrayMax(None, Option(func), None, array1, array2)

  def arrayMax3[I, O](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]],
                      array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[O] =
    ArrayMax(None, None, Option(func), array1, array2, array3)

  def arrayMin[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[O] =
    ArrayMin(func, None, None, array)

  def arrayMin2[I, O](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[O] =
    ArrayMin(None, Option(func), None, array1, array2)

  def arrayMin3[I, O](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]],
                      array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[O] =
    ArrayMin(None, None, Option(func), array1, array2, array3)

  def arrayReverseFill[I](func: TableColumn[I] => ExpressionColumn[Boolean],
                          array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[I]] =
    ArrayReverseFill(Option(func), None, None, array)

  def arrayReverseFill2[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                           array1: ArrayColMagnet[_ <: Iterable[I]],
                           array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[I]] =
    ArrayReverseFill(None, Option(func), None, array1, array2)

  def arrayReverseFill3[I](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                           array1: ArrayColMagnet[_ <: Iterable[I]],
                           array2: ArrayColMagnet[_ <: Iterable[I]],
                           array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[I]] =
    ArrayReverseFill(None, None, Option(func), array1, array2, array3)

  def arrayReverseSort[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                             array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArrayReverseSort(func, None, None, array)

  def arrayReverseSort2[I, O](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                              array1: ArrayColMagnet[_ <: Iterable[I]],
                              array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArrayReverseSort(None, Option(func), None, array1, array2)

  def arrayReverseSort3[I, O](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                              array1: ArrayColMagnet[_ <: Iterable[I]],
                              array2: ArrayColMagnet[_ <: Iterable[I]],
                              array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArrayReverseSort(None, None, Option(func), array1, array2, array3)

  def arrayReverseSplit[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                           array1: ArrayColMagnet[_ <: Iterable[I]],
                           array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[Iterable[I]]] =
    ArrayReverseSplit(func, array1, array2)

  def arraySort[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                      array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArraySort(func, None, None, array)

  def arraySort2[I, O](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                       array1: ArrayColMagnet[_ <: Iterable[I]],
                       array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArraySort(None, Option(func), None, array1, array2)

  def arraySort3[I, O](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                       array1: ArrayColMagnet[_ <: Iterable[I]],
                       array2: ArrayColMagnet[_ <: Iterable[I]],
                       array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[O]] =
    ArraySort(None, None, Option(func), array1, array2, array3)

  def arraySplit[I](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[Boolean],
                    array1: ArrayColMagnet[_ <: Iterable[I]],
                    array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[Iterable[Iterable[I]]] =
    ArraySplit(func, array1, array2)

  def arraySum[I, O](func: Option[TableColumn[I] => ExpressionColumn[O]],
                     array: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[O] =
    ArraySum(func, None, None, array)

  def arraySum2[I, O](func: (TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[O] =
    ArraySum(None, Option(func), None, array1, array2)

  def arraySum3[I, O](func: (TableColumn[I], TableColumn[I], TableColumn[I]) => ExpressionColumn[O],
                      array1: ArrayColMagnet[_ <: Iterable[I]],
                      array2: ArrayColMagnet[_ <: Iterable[I]],
                      array3: ArrayColMagnet[_ <: Iterable[I]]): ExpressionColumn[O] =
    ArraySum(None, None, Option(func), array1, array2, array3)
}
