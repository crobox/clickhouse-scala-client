package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn, Lambda, TableColumn}

trait HigherOrderFunctions { self: Magnets =>

  /**
   * A higher-order array function: its lambda, and the arrays the lambda's parameters are drawn from.
   *
   * The lambda is absent for the families where ClickHouse allows it to be omitted. The arrays are held with their
   * element types erased -- the type discipline belongs on the builders below, and rendering only reads `.column` off
   * them. `R` is the function's own result, which for the filtering and sorting families is the *first* array's element
   * type rather than the lambda's output.
   */
  abstract class HigherOrderFunction[O, R](
      val lambda: Option[Lambda[O]],
      val arrays: ArrayColMagnet[_]*
  ) extends ExpressionColumn[R](EmptyColumn)

  case class ArrayAll(_lambda: Option[Lambda[Boolean]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[Boolean, Boolean](_lambda, _arrays: _*)

  case class ArrayAvg[O](_lambda: Option[Lambda[O]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[O, Double](_lambda, _arrays: _*)

  case class ArrayCount(_lambda: Option[Lambda[Boolean]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[Boolean, Int](_lambda, _arrays: _*)

  case class ArrayCumSum[O](_lambda: Option[Lambda[O]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[O, Iterable[O]](_lambda, _arrays: _*)

  case class ArrayExists(_lambda: Option[Lambda[Boolean]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[Boolean, Boolean](_lambda, _arrays: _*)

  case class ArrayFill[I1](_lambda: Option[Lambda[Boolean]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[Boolean, Iterable[I1]](_lambda, _arrays: _*)

  case class ArrayFilter[I1](_lambda: Option[Lambda[Boolean]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[Boolean, Iterable[I1]](_lambda, _arrays: _*)

  case class ArrayFirst[I1](_lambda: Option[Lambda[Boolean]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[Boolean, I1](_lambda, _arrays: _*)

  case class ArrayFirstIndex(_lambda: Option[Lambda[Boolean]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[Boolean, Int](_lambda, _arrays: _*)

  case class ArrayMap[O](_lambda: Option[Lambda[O]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[O, Iterable[O]](_lambda, _arrays: _*)

  case class ArrayMax[O](_lambda: Option[Lambda[O]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[O, O](_lambda, _arrays: _*)

  case class ArrayMin[O](_lambda: Option[Lambda[O]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[O, O](_lambda, _arrays: _*)

  case class ArrayReverseFill[I1](_lambda: Option[Lambda[Boolean]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[Boolean, Iterable[I1]](_lambda, _arrays: _*)

  case class ArrayReverseSort[I1, O](_lambda: Option[Lambda[O]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[O, Iterable[I1]](_lambda, _arrays: _*)

  case class ArrayReverseSplit[I1](_lambda: Option[Lambda[Boolean]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[Boolean, Iterable[Iterable[I1]]](_lambda, _arrays: _*)

  case class ArraySort[I1, O](_lambda: Option[Lambda[O]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[O, Iterable[I1]](_lambda, _arrays: _*)

  case class ArraySplit[I1](_lambda: Option[Lambda[Boolean]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[Boolean, Iterable[Iterable[I1]]](_lambda, _arrays: _*)

  case class ArraySum[O](_lambda: Option[Lambda[O]], _arrays: ArrayColMagnet[_]*)
      extends HigherOrderFunction[O, O](_lambda, _arrays: _*)

  def arrayAll[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Boolean] =
    ArrayAll(Option(Lambda.Of1(func)), array)

  def arrayAll[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Boolean] =
    ArrayAll(Option(Lambda.Of2(func)), array1, array2)

  def arrayAll[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Boolean] =
    ArrayAll(Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayAll[I1, I2, I3, I4](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Boolean] =
    ArrayAll(Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayAll[I1, I2, I3, I4, I5](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        Boolean
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Boolean] =
    ArrayAll(Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayAvg[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Double] =
    ArrayAvg[O](func.map(Lambda.Of1(_)), array)

  def arrayAvg[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Double] =
    ArrayAvg[O](Option(Lambda.Of2(func)), array1, array2)

  def arrayAvg[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Double] =
    ArrayAvg[O](Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayAvg[I1, I2, I3, I4, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Double] =
    ArrayAvg[O](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayAvg[I1, I2, I3, I4, I5, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        O
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Double] =
    ArrayAvg[O](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayCount[I](
      func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Int] =
    ArrayCount(func.map(Lambda.Of1(_)), array)

  def arrayCount[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Int] =
    ArrayCount(Option(Lambda.Of2(func)), array1, array2)

  def arrayCount[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Int] =
    ArrayCount(Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayCount[I1, I2, I3, I4](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Int] =
    ArrayCount(Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayCount[I1, I2, I3, I4, I5](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        Boolean
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Int] =
    ArrayCount(Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayCumSum[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayCumSum[O](func.map(Lambda.Of1(_)), array)

  def arrayCumSum[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayCumSum[O](Option(Lambda.Of2(func)), array1, array2)

  def arrayCumSum[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayCumSum[O](Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayCumSum[I1, I2, I3, I4, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayCumSum[O](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayCumSum[I1, I2, I3, I4, I5, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        O
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayCumSum[O](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayExists[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Boolean] =
    ArrayExists(Option(Lambda.Of1(func)), array)

  def arrayExists[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Boolean] =
    ArrayExists(Option(Lambda.Of2(func)), array1, array2)

  def arrayExists[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Boolean] =
    ArrayExists(Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayExists[I1, I2, I3, I4](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Boolean] =
    ArrayExists(Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayExists[I1, I2, I3, I4, I5](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        Boolean
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Boolean] =
    ArrayExists(Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayFill[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[I]] =
    ArrayFill[I](Option(Lambda.Of1(func)), array)

  def arrayFill[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFill[I1](Option(Lambda.Of2(func)), array1, array2)

  def arrayFill[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFill[I1](Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayFill[I1, I2, I3, I4](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFill[I1](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayFill[I1, I2, I3, I4, I5](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        Boolean
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFill[I1](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayFilter[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[I]] =
    ArrayFilter[I](Option(Lambda.Of1(func)), array)

  def arrayFilter[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFilter[I1](Option(Lambda.Of2(func)), array1, array2)

  def arrayFilter[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFilter[I1](Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayFilter[I1, I2, I3, I4](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFilter[I1](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayFilter[I1, I2, I3, I4, I5](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        Boolean
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFilter[I1](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayFirst[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[I] =
    ArrayFirst[I](Option(Lambda.Of1(func)), array)

  def arrayFirst[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[I1] =
    ArrayFirst[I1](Option(Lambda.Of2(func)), array1, array2)

  def arrayFirst[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[I1] =
    ArrayFirst[I1](Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayFirst[I1, I2, I3, I4](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[I1] =
    ArrayFirst[I1](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayFirst[I1, I2, I3, I4, I5](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        Boolean
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[I1] =
    ArrayFirst[I1](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayFirstIndex[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Int] =
    ArrayFirstIndex(Option(Lambda.Of1(func)), array)

  def arrayFirstIndex[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Int] =
    ArrayFirstIndex(Option(Lambda.Of2(func)), array1, array2)

  def arrayFirstIndex[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Int] =
    ArrayFirstIndex(Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayFirstIndex[I1, I2, I3, I4](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Int] =
    ArrayFirstIndex(Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayFirstIndex[I1, I2, I3, I4, I5](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        Boolean
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Int] =
    ArrayFirstIndex(Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayMap[I, O](
      func: TableColumn[I] => ExpressionColumn[O],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayMap[O](Option(Lambda.Of1(func)), array)

  def arrayMap[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayMap[O](Option(Lambda.Of2(func)), array1, array2)

  def arrayMap[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayMap[O](Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayMap[I1, I2, I3, I4, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayMap[O](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayMap[I1, I2, I3, I4, I5, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        O
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayMap[O](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayMax[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[O] =
    ArrayMax[O](func.map(Lambda.Of1(_)), array)

  def arrayMax[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[O] =
    ArrayMax[O](Option(Lambda.Of2(func)), array1, array2)

  def arrayMax[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[O] =
    ArrayMax[O](Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayMax[I1, I2, I3, I4, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[O] =
    ArrayMax[O](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayMax[I1, I2, I3, I4, I5, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        O
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[O] =
    ArrayMax[O](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayMin[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[O] =
    ArrayMin[O](func.map(Lambda.Of1(_)), array)

  def arrayMin[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[O] =
    ArrayMin[O](Option(Lambda.Of2(func)), array1, array2)

  def arrayMin[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[O] =
    ArrayMin[O](Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayMin[I1, I2, I3, I4, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[O] =
    ArrayMin[O](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayMin[I1, I2, I3, I4, I5, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        O
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[O] =
    ArrayMin[O](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayReverseFill[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[I]] =
    ArrayReverseFill[I](Option(Lambda.Of1(func)), array)

  def arrayReverseFill[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseFill[I1](Option(Lambda.Of2(func)), array1, array2)

  def arrayReverseFill[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseFill[I1](Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayReverseFill[I1, I2, I3, I4](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseFill[I1](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayReverseFill[I1, I2, I3, I4, I5](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        Boolean
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseFill[I1](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayReverseSort[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[I]] =
    ArrayReverseSort[I, O](func.map(Lambda.Of1(_)), array)

  def arrayReverseSort[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseSort[I1, O](Option(Lambda.Of2(func)), array1, array2)

  def arrayReverseSort[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseSort[I1, O](Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayReverseSort[I1, I2, I3, I4, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseSort[I1, O](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayReverseSort[I1, I2, I3, I4, I5, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        O
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseSort[I1, O](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arrayReverseSplit[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[Iterable[I]]] =
    ArrayReverseSplit[I](Option(Lambda.Of1(func)), array)

  def arrayReverseSplit[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArrayReverseSplit[I1](Option(Lambda.Of2(func)), array1, array2)

  def arrayReverseSplit[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArrayReverseSplit[I1](Option(Lambda.Of3(func)), array1, array2, array3)

  def arrayReverseSplit[I1, I2, I3, I4](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArrayReverseSplit[I1](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arrayReverseSplit[I1, I2, I3, I4, I5](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        Boolean
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArrayReverseSplit[I1](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arraySort[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[I]] =
    ArraySort[I, O](func.map(Lambda.Of1(_)), array)

  def arraySort[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[I1]] =
    ArraySort[I1, O](Option(Lambda.Of2(func)), array1, array2)

  def arraySort[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[I1]] =
    ArraySort[I1, O](Option(Lambda.Of3(func)), array1, array2, array3)

  def arraySort[I1, I2, I3, I4, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Iterable[I1]] =
    ArraySort[I1, O](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arraySort[I1, I2, I3, I4, I5, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        O
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Iterable[I1]] =
    ArraySort[I1, O](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arraySplit[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[Iterable[I]]] =
    ArraySplit[I](Option(Lambda.Of1(func)), array)

  def arraySplit[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArraySplit[I1](Option(Lambda.Of2(func)), array1, array2)

  def arraySplit[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArraySplit[I1](Option(Lambda.Of3(func)), array1, array2, array3)

  def arraySplit[I1, I2, I3, I4](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArraySplit[I1](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arraySplit[I1, I2, I3, I4, I5](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        Boolean
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArraySplit[I1](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

  def arraySum[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[O] =
    ArraySum[O](func.map(Lambda.Of1(_)), array)

  def arraySum[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[O] =
    ArraySum[O](Option(Lambda.Of2(func)), array1, array2)

  def arraySum[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[O] =
    ArraySum[O](Option(Lambda.Of3(func)), array1, array2, array3)

  def arraySum[I1, I2, I3, I4, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]]
  ): ExpressionColumn[O] =
    ArraySum[O](Option(Lambda.Of4(func)), array1, array2, array3, array4)

  def arraySum[I1, I2, I3, I4, I5, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[
        O
      ],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]],
      array4: ArrayColMagnet[_ <: Iterable[I4]],
      array5: ArrayColMagnet[_ <: Iterable[I5]]
  ): ExpressionColumn[O] =
    ArraySum[O](Option(Lambda.Of5(func)), array1, array2, array3, array4, array5)

}
