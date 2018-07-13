package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._
import org.joda.time.{DateTime, LocalDate}

trait ArrayFunctions { this: Magnets =>
  sealed trait ArrayFunction

  abstract class ArrayFunctionOp[V](col: ArrayColMagnet)
    extends ExpressionColumn[V](col.column)
    with ArrayFunction

  abstract class ArrayFunctionConst[V]
    extends ExpressionColumn[Iterable[V]](EmptyColumn())
    with ArrayFunction

  case class EmptyArrayUInt8()  extends ArrayFunctionConst[Long]
  case class EmptyArrayUInt16() extends ArrayFunctionConst[Long]
  case class EmptyArrayUInt32() extends ArrayFunctionConst[Long]
  case class EmptyArrayUInt64() extends ArrayFunctionConst[Long]
  case class EmptyArrayInt8()   extends ArrayFunctionConst[Long]
  case class EmptyArrayInt16()  extends ArrayFunctionConst[Long]
  case class EmptyArrayInt32()  extends ArrayFunctionConst[Long]
  case class EmptyArrayInt64()  extends ArrayFunctionConst[Long]
  case class EmptyArrayFloat32() extends ArrayFunctionConst[Float]
  case class EmptyArrayFloat64() extends ArrayFunctionConst[Float]
  case class EmptyArrayDate()   extends ArrayFunctionConst[LocalDate]
  case class EmptyArrayDateTime() extends ArrayFunctionConst[DateTime]
  case class EmptyArrayString() extends ArrayFunctionConst[String]
  case class Range[V](n: NumericCol)   extends ArrayFunctionConst[V]

  case class EmptyArrayToSingle[V](col: ArrayColMagnet) extends ArrayFunctionOp[V](col)
  case class Array[V](col1: ArrayColMagnet, coln: ArrayColMagnet*) extends ArrayFunctionOp[V](col1)
  case class ArrayConcat(col1: ArrayColMagnet, col2: ArrayColMagnet,coln: ArrayColMagnet*) extends ArrayFunctionOp(col1)
  case class ArrayElement(col: ArrayColMagnet, n: NumericCol) extends ArrayFunctionOp(col)
  case class Has(col: ArrayColMagnet, elm: Magnet) extends ArrayFunctionOp(col)
  case class IndexOf(col: ArrayColMagnet, elm: ConstOrColMagnet) extends ArrayFunctionOp(col)
  case class CountEqual(col: ArrayColMagnet, elm: ConstOrColMagnet) extends ArrayFunctionOp(col)
  case class ArrayEnumerate(col: ArrayColMagnet) extends ArrayFunctionOp(col)
  case class ArrayEnumerateUniq(col1: ArrayColMagnet,coln: ArrayColMagnet*) extends ArrayFunctionOp(col1)
  case class ArrayPopBack(col: ArrayColMagnet) extends ArrayFunctionOp(col)
  case class ArrayPopFront(col: ArrayColMagnet) extends ArrayFunctionOp(col)
  case class ArrayPushBack(col: ArrayColMagnet, elm: ConstOrColMagnet) extends ArrayFunctionOp(col)
  case class ArrayPushFront(col: ArrayColMagnet, elm: ConstOrColMagnet) extends ArrayFunctionOp(col)
  case class ArraySlice(col: ArrayColMagnet, offset: NumericCol, length: NumericCol = 0) extends ArrayFunctionOp(col)
  case class ArrayUniq(col1: ArrayColMagnet,coln: ArrayColMagnet*) extends ArrayFunctionOp(col1)
  case class ArrayJoin(col: ArrayColMagnet) extends ArrayFunctionOp(col)

  def emptyArrayUInt8 = EmptyArrayUInt8()
  def emptyArrayUInt16 = EmptyArrayUInt16()
  def emptyArrayUInt32 = EmptyArrayUInt32()
  def emptyArrayUInt64 = EmptyArrayUInt64()
  def emptyArrayInt8 = EmptyArrayInt8()
  def emptyArrayInt16 = EmptyArrayInt16()
  def emptyArrayInt32 = EmptyArrayInt32()
  def emptyArrayInt64 = EmptyArrayInt64()
  def emptyArrayFloat32 = EmptyArrayFloat32()
  def emptyArrayFloat64 = EmptyArrayFloat64()
  def emptyArrayDate = EmptyArrayDate()
  def emptyArrayDateTime = EmptyArrayDateTime()
  def emptyArrayString = EmptyArrayString()
  def emptyArrayToSingle(col: ArrayColMagnet) = EmptyArrayToSingle[col.ColumnType](col)
  def range(n: NumericCol) = Range(n)


  def arrayConcat(col1: ArrayColMagnet,col2: ArrayColMagnet,coln: ArrayColMagnet*) = ArrayConcat(col1, col2, coln:_*)
  def arrayElement(col: ArrayColMagnet, n: NumericCol) = ArrayElement(col, n)
  def has(col: ArrayColMagnet, elm: ConstOrColMagnet) = Has(col, elm)
  def indexOf(col: ArrayColMagnet, elm: ConstOrColMagnet) = IndexOf(col, elm)
  def countEqual(col: ArrayColMagnet, elm: ConstOrColMagnet) = CountEqual(col, elm)
  def arrayEnumerate(col: ArrayColMagnet) = ArrayEnumerate(col)
  def arrayEnumerateUniq(col1: ArrayColMagnet,coln: ArrayColMagnet*) = ArrayEnumerateUniq(col1, coln:_*)
  def arrayPopBack(col: ArrayColMagnet) = ArrayPopBack(col)
  def arrayPopFront(col: ArrayColMagnet) = ArrayPopFront(col)
  def arrayPushBack(col: ArrayColMagnet, elm: ConstOrColMagnet) = ArrayPushBack(col, elm)
  def arrayPushFront(col: ArrayColMagnet, elm: ConstOrColMagnet) = ArrayPushFront(col, elm)
  def arraySlice(col: ArrayColMagnet, offset: NumericCol, length: NumericCol = 0) = ArraySlice(col, offset, length)
  def arrayUniq(col1: ArrayColMagnet,coln: ArrayColMagnet*) = ArrayUniq(col1, coln:_*)
  def arrayJoin(col: ArrayColMagnet) = ArrayJoin(col)

  /*
emptyArrayUInt8,
emptyArrayUInt16,
emptyArrayUInt32,
emptyArrayUInt64
emptyArrayInt8,
emptyArrayInt16,
emptyArrayInt32,
emptyArrayInt64
emptyArrayFloat32,
emptyArrayFloat64
emptyArrayDate,
emptyArrayDateTime
emptyArrayString
emptyArrayToSingle
range(N)
*array(x1, ...),
*operator [x1, ...]
arrayConcat
arrayElement(arr, n),
operator arr[n]
has(arr, elem)
indexOf(arr, x)
countEqual(arr, x)
arrayEnumerate(arr)
arrayEnumerateUniq(arr, ...)
arrayPopBack
arrayPopFront
arrayPushBack
arrayPushFront
arraySlice
arrayUniq(arr, ...)
arrayJoin(arr)
 */
}
