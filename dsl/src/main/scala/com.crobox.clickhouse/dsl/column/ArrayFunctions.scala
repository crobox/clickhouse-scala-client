package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._
import org.joda.time.{DateTime, LocalDate}

trait ArrayFunctions { this: Magnets =>
  sealed trait ArrayFunction

  abstract class ArrayFunctionOp[V] extends ExpressionColumn[V](EmptyColumn) with ArrayFunction

  abstract class ArrayFunctionConst[V] extends ExpressionColumn[Iterable[V]](EmptyColumn) with ArrayFunction

  case class EmptyArrayUInt8()       extends ArrayFunctionConst[Boolean]
  case class EmptyArrayUInt16()      extends ArrayFunctionConst[Short]
  case class EmptyArrayUInt32()      extends ArrayFunctionConst[Int]
  case class EmptyArrayUInt64()      extends ArrayFunctionConst[Long]
  case class EmptyArrayInt8()        extends ArrayFunctionConst[Boolean]
  case class EmptyArrayInt16()       extends ArrayFunctionConst[Short]
  case class EmptyArrayInt32()       extends ArrayFunctionConst[Int]
  case class EmptyArrayInt64()       extends ArrayFunctionConst[Long]
  case class EmptyArrayFloat32()     extends ArrayFunctionConst[Float]
  case class EmptyArrayFloat64()     extends ArrayFunctionConst[Double]
  case class EmptyArrayDate()        extends ArrayFunctionConst[LocalDate]
  case class EmptyArrayDateTime()    extends ArrayFunctionConst[DateTime]
  case class EmptyArrayString()      extends ArrayFunctionConst[String]
  case class Range(n: NumericCol[_]) extends ArrayFunctionConst[Long]

  case class EmptyArrayToSingle[V](col: ArrayColMagnet[V])                   extends ArrayFunctionOp[V]
  case class Array[V](col1: ConstOrColMagnet[V], coln: ConstOrColMagnet[V]*) extends ArrayFunctionOp[Iterable[V]]
  case class ArrayConcat[V](col1: ArrayColMagnet[V], col2: ArrayColMagnet[V], coln: ArrayColMagnet[V]*)
      extends ArrayFunctionOp[Iterable[V]]
  case class ArrayElement[V](col: ArrayColMagnet[_ <: Iterable[V]], n: NumericCol[_])    extends ArrayFunctionOp[V]
  case class Has(col: ArrayColMagnet[_], elm: Magnet[_])                                 extends ArrayFunctionOp[Boolean]
  case class HasAll(col: ArrayColMagnet[_], elm: Magnet[_])                              extends ArrayFunctionOp[Boolean]
  case class HasAny(col: ArrayColMagnet[_], elm: Magnet[_])                              extends ArrayFunctionOp[Boolean]
  case class IndexOf[V](col: ArrayColMagnet[_ <: Iterable[V]], elm: ConstOrColMagnet[V]) extends ArrayFunctionOp[Long]
  case class CountEqual[V](col: ArrayColMagnet[_ <: Iterable[V]], elm: ConstOrColMagnet[V])
      extends ArrayFunctionOp[Long]
  case class ArrayEnumerate(col: ArrayColMagnet[_]) extends ArrayFunctionOp[Iterable[Long]]
  case class ArrayEnumerateUniq[V](col1: ArrayColMagnet[_ <: Iterable[V]], coln: ArrayColMagnet[_ <: Iterable[V]]*)
      extends ArrayFunctionOp[Iterable[Long]]
  case class ArrayPopBack[V](col: ArrayColMagnet[_ <: Iterable[V]])  extends ArrayFunctionOp[Iterable[V]]
  case class ArrayPopFront[V](col: ArrayColMagnet[_ <: Iterable[V]]) extends ArrayFunctionOp[Iterable[V]]
  case class ArrayPushBack[V](col: ArrayColMagnet[_ <: Iterable[V]], elm: ConstOrColMagnet[V])
      extends ArrayFunctionOp[Iterable[V]]
  case class ArrayPushFront[V](col: ArrayColMagnet[_ <: Iterable[V]], elm: ConstOrColMagnet[V])
      extends ArrayFunctionOp[Iterable[V]]
  case class ArrayResize[V](col: ArrayColMagnet[_ <: Iterable[V]], size: NumericCol[_], extender: ConstOrColMagnet[V])
      extends ArrayFunctionOp[Iterable[V]]
  case class ArraySlice[V](col: ArrayColMagnet[_ <: Iterable[V]], offset: NumericCol[_], length: NumericCol[_] = 0)
      extends ArrayFunctionOp[Iterable[V]]
  case class ArrayUniq[V](col1: ArrayColMagnet[_ <: Iterable[V]], coln: ArrayColMagnet[_ <: Iterable[V]]*)
      extends ArrayFunctionOp[Long]
  case class ArrayJoin[V](col: ArrayColMagnet[_ <: Iterable[V]]) extends ArrayFunctionOp[V]

  def emptyArrayUInt8: EmptyArrayUInt8                                     = EmptyArrayUInt8()
  def emptyArrayUInt16: EmptyArrayUInt16                                   = EmptyArrayUInt16()
  def emptyArrayUInt32: EmptyArrayUInt32                                   = EmptyArrayUInt32()
  def emptyArrayUInt64: EmptyArrayUInt64                                   = EmptyArrayUInt64()
  def emptyArrayInt8: EmptyArrayInt8                                       = EmptyArrayInt8()
  def emptyArrayInt16: EmptyArrayInt16                                     = EmptyArrayInt16()
  def emptyArrayInt32: EmptyArrayInt32                                     = EmptyArrayInt32()
  def emptyArrayInt64: EmptyArrayInt64                                     = EmptyArrayInt64()
  def emptyArrayFloat32: EmptyArrayFloat32                                 = EmptyArrayFloat32()
  def emptyArrayFloat64: EmptyArrayFloat64                                 = EmptyArrayFloat64()
  def emptyArrayDate: EmptyArrayDate                                       = EmptyArrayDate()
  def emptyArrayDateTime: EmptyArrayDateTime                               = EmptyArrayDateTime()
  def emptyArrayString: EmptyArrayString                                   = EmptyArrayString()
  def emptyArrayToSingle[V](col: ArrayColMagnet[V]): EmptyArrayToSingle[V] = EmptyArrayToSingle[V](col)
  def range(n: NumericCol[_]): Range                                       = Range(n)

  def arrayConcat[V](col1: ArrayColMagnet[V], col2: ArrayColMagnet[V], coln: ArrayColMagnet[V]*): ArrayConcat[V] =
    ArrayConcat(col1, col2, coln: _*)
  def arrayElement[V](col: ArrayColMagnet[_ <: Iterable[V]], n: NumericCol[_]): ArrayElement[V] = ArrayElement(col, n)
  def has(col: ArrayColMagnet[_], elm: ConstOrColMagnet[_]): Has                                = Has(col, elm)
  def hasAll(col: ArrayColMagnet[_], elm: ArrayColMagnet[_]): HasAll                            = HasAll(col, elm)
  def hasAny(col: ArrayColMagnet[_], elm: ArrayColMagnet[_]): HasAny                            = HasAny(col, elm)
  def indexOf[V](col: ArrayColMagnet[_ <: Iterable[V]], elm: ConstOrColMagnet[V]): IndexOf[V]   = IndexOf[V](col, elm)

  def countEqual[V](col: ArrayColMagnet[_ <: Iterable[V]], elm: ConstOrColMagnet[V]): CountEqual[V] =
    CountEqual[V](col, elm)
  def arrayEnumerate(col: ArrayColMagnet[_]): ArrayEnumerate = ArrayEnumerate(col)

  def arrayEnumerateUniq[V](col1: ArrayColMagnet[_ <: Iterable[V]],
                            coln: ArrayColMagnet[_ <: Iterable[V]]*): ArrayEnumerateUniq[V] =
    ArrayEnumerateUniq[V](col1, coln: _*)
  def arrayPopBack[V](col: ArrayColMagnet[_ <: Iterable[V]]): ArrayPopBack[V]   = ArrayPopBack[V](col)
  def arrayPopFront[V](col: ArrayColMagnet[_ <: Iterable[V]]): ArrayPopFront[V] = ArrayPopFront[V](col)

  def arrayPushBack[V](col: ArrayColMagnet[_ <: Iterable[V]], elm: ConstOrColMagnet[V]): ArrayPushBack[V] =
    ArrayPushBack[V](col, elm)

  def arrayPushFront[V](col: ArrayColMagnet[_ <: Iterable[V]], elm: ConstOrColMagnet[V]): ArrayPushFront[V] =
    ArrayPushFront[V](col, elm)

  def arrayResize[V](col: ArrayColMagnet[_ <: Iterable[V]],
                     size: NumericCol[_],
                     extender: ConstOrColMagnet[V]): ArrayResize[V] =
    ArrayResize[V](col, size, extender)

  def arraySlice[V](col: ArrayColMagnet[_ <: Iterable[V]],
                    offset: NumericCol[_],
                    length: NumericCol[_] = 0): ArraySlice[V] =
    ArraySlice[V](col, offset, length)

  def arrayUniq[V](col1: ArrayColMagnet[_ <: Iterable[V]], coln: ArrayColMagnet[_ <: Iterable[V]]*): ArrayUniq[V] =
    ArrayUniq[V](col1, coln: _*)
  def arrayJoin[V](col: ArrayColMagnet[_ <: Iterable[V]]): ArrayJoin[V] = ArrayJoin[V](col)
}
