package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn, TableColumn}

trait HigherOrderFunctions { self: Magnets =>

  /**
   * A higher-order array function: a lambda plus the arrays its parameters are drawn from.
   *
   * Each array has its own element type. ClickHouse does not require them to agree -- `arrayMap((id, price, qty) ->
   * ..., ids, prices, quantities)` over `String`, `Float64` and `UInt32` is ordinary usage -- and binding them to one
   * type made any such expression unwritable, with no cast able to unify a String with a numeric.
   *
   * Exactly one of the three lambda slots is set, and the number of arrays matches its arity. `I2` and `I3` are
   * `Nothing` for the arities that do not reach them.
   *
   * The arrays are held with their element types erased: the type discipline belongs on the builders below, and the
   * tokenizer only ever reads `.column` off them.
   */
  abstract class HigherOrderFunction[I1, I2, I3, O, R](
      val func1: Option[TableColumn[I1] => ExpressionColumn[O]],
      val func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O]],
      val func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O]],
      val arrays: ArrayColMagnet[_]*
  ) extends ExpressionColumn[R](EmptyColumn)

  case class ArrayAll[I1, I2, I3](
      _func1: Option[TableColumn[I1] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, Boolean, Boolean](_func1, _func2, _func3, _arrays: _*)

  case class ArrayAvg[I1, I2, I3, O](
      _func1: Option[TableColumn[I1] => ExpressionColumn[O]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, O, Double](_func1, _func2, _func3, _arrays: _*)

  case class ArrayCount[I1, I2, I3](
      _func1: Option[TableColumn[I1] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, Boolean, Int](_func1, _func2, _func3, _arrays: _*)

  case class ArrayCumSum[I1, I2, I3, O](
      _func1: Option[TableColumn[I1] => ExpressionColumn[O]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, O, Iterable[O]](_func1, _func2, _func3, _arrays: _*)

  case class ArrayExists[I1, I2, I3](
      _func1: Option[TableColumn[I1] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, Boolean, Boolean](_func1, _func2, _func3, _arrays: _*)

  case class ArrayFill[I1, I2, I3](
      _func1: Option[TableColumn[I1] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, Boolean, Iterable[I1]](_func1, _func2, _func3, _arrays: _*)

  case class ArrayFilter[I1, I2, I3](
      _func1: Option[TableColumn[I1] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, Boolean, Iterable[I1]](_func1, _func2, _func3, _arrays: _*)

  case class ArrayFirst[I1, I2, I3](
      _func1: Option[TableColumn[I1] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, Boolean, I1](_func1, _func2, _func3, _arrays: _*)

  case class ArrayFirstIndex[I1, I2, I3](
      _func1: Option[TableColumn[I1] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, Boolean, Int](_func1, _func2, _func3, _arrays: _*)

  case class ArrayMap[I1, I2, I3, O](
      _func1: Option[TableColumn[I1] => ExpressionColumn[O]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, O, Iterable[O]](_func1, _func2, _func3, _arrays: _*)

  case class ArrayMax[I1, I2, I3, O](
      _func1: Option[TableColumn[I1] => ExpressionColumn[O]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, O, O](_func1, _func2, _func3, _arrays: _*)

  case class ArrayMin[I1, I2, I3, O](
      _func1: Option[TableColumn[I1] => ExpressionColumn[O]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, O, O](_func1, _func2, _func3, _arrays: _*)

  case class ArrayReverseFill[I1, I2, I3](
      _func1: Option[TableColumn[I1] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, Boolean, Iterable[I1]](_func1, _func2, _func3, _arrays: _*)

  case class ArrayReverseSort[I1, I2, I3, O](
      _func1: Option[TableColumn[I1] => ExpressionColumn[O]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, O, Iterable[I1]](_func1, _func2, _func3, _arrays: _*)

  case class ArrayReverseSplit[I1, I2, I3](
      _func1: Option[TableColumn[I1] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, Boolean, Iterable[Iterable[I1]]](_func1, _func2, _func3, _arrays: _*)

  case class ArraySort[I1, I2, I3, O](
      _func1: Option[TableColumn[I1] => ExpressionColumn[O]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, O, Iterable[I1]](_func1, _func2, _func3, _arrays: _*)

  case class ArraySplit[I1, I2, I3](
      _func1: Option[TableColumn[I1] => ExpressionColumn[Boolean]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, Boolean, Iterable[Iterable[I1]]](_func1, _func2, _func3, _arrays: _*)

  case class ArraySum[I1, I2, I3, O](
      _func1: Option[TableColumn[I1] => ExpressionColumn[O]],
      _func2: Option[(TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O]],
      _func3: Option[(TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O]],
      _arrays: ArrayColMagnet[_]*
  ) extends HigherOrderFunction[I1, I2, I3, O, O](_func1, _func2, _func3, _arrays: _*)

  def arrayAll[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Boolean] =
    ArrayAll[I, Nothing, Nothing](Option(func), None, None, array)

  def arrayAll2[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Boolean] =
    ArrayAll[I1, I2, Nothing](None, Option(func), None, array1, array2)

  def arrayAll3[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Boolean] =
    ArrayAll[I1, I2, I3](None, None, Option(func), array1, array2, array3)

  def arrayAvg[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Double] =
    ArrayAvg[I, Nothing, Nothing, O](func, None, None, array)

  def arrayAvg2[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Double] =
    ArrayAvg[I1, I2, Nothing, O](None, Option(func), None, array1, array2)

  def arrayAvg3[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Double] =
    ArrayAvg[I1, I2, I3, O](None, None, Option(func), array1, array2, array3)

  def arrayCount[I](
      func: Option[TableColumn[I] => ExpressionColumn[Boolean]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Int] =
    ArrayCount[I, Nothing, Nothing](func, None, None, array)

  def arrayCount2[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Int] =
    ArrayCount[I1, I2, Nothing](None, Option(func), None, array1, array2)

  def arrayCount3[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Int] =
    ArrayCount[I1, I2, I3](None, None, Option(func), array1, array2, array3)

  def arrayCumSum[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayCumSum[I, Nothing, Nothing, O](func, None, None, array)

  def arrayCumSum2[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayCumSum[I1, I2, Nothing, O](None, Option(func), None, array1, array2)

  def arrayCumSum3[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayCumSum[I1, I2, I3, O](None, None, Option(func), array1, array2, array3)

  def arrayExists[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Boolean] =
    ArrayExists[I, Nothing, Nothing](Option(func), None, None, array)

  def arrayExists2[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Boolean] =
    ArrayExists[I1, I2, Nothing](None, Option(func), None, array1, array2)

  def arrayExists3[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Boolean] =
    ArrayExists[I1, I2, I3](None, None, Option(func), array1, array2, array3)

  def arrayFill[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[I]] =
    ArrayFill[I, Nothing, Nothing](Option(func), None, None, array)

  def arrayFill2[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFill[I1, I2, Nothing](None, Option(func), None, array1, array2)

  def arrayFill3[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFill[I1, I2, I3](None, None, Option(func), array1, array2, array3)

  def arrayFilter[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[I]] =
    ArrayFilter[I, Nothing, Nothing](Option(func), None, None, array)

  def arrayFilter2[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFilter[I1, I2, Nothing](None, Option(func), None, array1, array2)

  def arrayFilter3[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayFilter[I1, I2, I3](None, None, Option(func), array1, array2, array3)

  def arrayFirst[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[I] =
    ArrayFirst[I, Nothing, Nothing](Option(func), None, None, array)

  def arrayFirst2[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[I1] =
    ArrayFirst[I1, I2, Nothing](None, Option(func), None, array1, array2)

  def arrayFirst3[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[I1] =
    ArrayFirst[I1, I2, I3](None, None, Option(func), array1, array2, array3)

  def arrayFirstIndex[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Int] =
    ArrayFirstIndex[I, Nothing, Nothing](Option(func), None, None, array)

  def arrayFirstIndex2[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Int] =
    ArrayFirstIndex[I1, I2, Nothing](None, Option(func), None, array1, array2)

  def arrayFirstIndex3[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Int] =
    ArrayFirstIndex[I1, I2, I3](None, None, Option(func), array1, array2, array3)

  def arrayMap[I, O](
      func: TableColumn[I] => ExpressionColumn[O],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayMap[I, Nothing, Nothing, O](Option(func), None, None, array)

  def arrayMap2[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayMap[I1, I2, Nothing, O](None, Option(func), None, array1, array2)

  def arrayMap3[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[O]] =
    ArrayMap[I1, I2, I3, O](None, None, Option(func), array1, array2, array3)

  def arrayMax[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[O] =
    ArrayMax[I, Nothing, Nothing, O](func, None, None, array)

  def arrayMax2[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[O] =
    ArrayMax[I1, I2, Nothing, O](None, Option(func), None, array1, array2)

  def arrayMax3[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[O] =
    ArrayMax[I1, I2, I3, O](None, None, Option(func), array1, array2, array3)

  def arrayMin[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[O] =
    ArrayMin[I, Nothing, Nothing, O](func, None, None, array)

  def arrayMin2[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[O] =
    ArrayMin[I1, I2, Nothing, O](None, Option(func), None, array1, array2)

  def arrayMin3[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[O] =
    ArrayMin[I1, I2, I3, O](None, None, Option(func), array1, array2, array3)

  def arrayReverseFill[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[I]] =
    ArrayReverseFill[I, Nothing, Nothing](Option(func), None, None, array)

  def arrayReverseFill2[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseFill[I1, I2, Nothing](None, Option(func), None, array1, array2)

  def arrayReverseFill3[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseFill[I1, I2, I3](None, None, Option(func), array1, array2, array3)

  def arrayReverseSort[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[I]] =
    ArrayReverseSort[I, Nothing, Nothing, O](func, None, None, array)

  def arrayReverseSort2[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseSort[I1, I2, Nothing, O](None, Option(func), None, array1, array2)

  def arrayReverseSort3[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[I1]] =
    ArrayReverseSort[I1, I2, I3, O](None, None, Option(func), array1, array2, array3)

  def arrayReverseSplit[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[Iterable[I]]] =
    ArrayReverseSplit[I, Nothing, Nothing](Option(func), None, None, array)

  def arrayReverseSplit2[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArrayReverseSplit[I1, I2, Nothing](None, Option(func), None, array1, array2)

  def arrayReverseSplit3[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArrayReverseSplit[I1, I2, I3](None, None, Option(func), array1, array2, array3)

  def arraySort[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[I]] =
    ArraySort[I, Nothing, Nothing, O](func, None, None, array)

  def arraySort2[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[I1]] =
    ArraySort[I1, I2, Nothing, O](None, Option(func), None, array1, array2)

  def arraySort3[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[I1]] =
    ArraySort[I1, I2, I3, O](None, None, Option(func), array1, array2, array3)

  def arraySplit[I](
      func: TableColumn[I] => ExpressionColumn[Boolean],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[Iterable[Iterable[I]]] =
    ArraySplit[I, Nothing, Nothing](Option(func), None, None, array)

  def arraySplit2[I1, I2](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArraySplit[I1, I2, Nothing](None, Option(func), None, array1, array2)

  def arraySplit3[I1, I2, I3](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[Boolean],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[Iterable[Iterable[I1]]] =
    ArraySplit[I1, I2, I3](None, None, Option(func), array1, array2, array3)

  def arraySum[I, O](
      func: Option[TableColumn[I] => ExpressionColumn[O]],
      array: ArrayColMagnet[_ <: Iterable[I]]
  ): ExpressionColumn[O] =
    ArraySum[I, Nothing, Nothing, O](func, None, None, array)

  def arraySum2[I1, I2, O](
      func: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]]
  ): ExpressionColumn[O] =
    ArraySum[I1, I2, Nothing, O](None, Option(func), None, array1, array2)

  def arraySum3[I1, I2, I3, O](
      func: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O],
      array1: ArrayColMagnet[_ <: Iterable[I1]],
      array2: ArrayColMagnet[_ <: Iterable[I2]],
      array3: ArrayColMagnet[_ <: Iterable[I3]]
  ): ExpressionColumn[O] =
    ArraySum[I1, I2, I3, O](None, None, Option(func), array1, array2, array3)

}
