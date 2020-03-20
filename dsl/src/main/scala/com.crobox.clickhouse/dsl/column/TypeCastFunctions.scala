package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{ExpressionColumn, TableColumn}
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType.SimpleColumnType

import scala.reflect.ClassTag

trait TypeCastFunctions { self: Magnets =>

  abstract class TypeCastColumn[V](val targetColumn: ConstOrColMagnet[_]) extends ExpressionColumn[V](targetColumn.column)

  case class Reinterpret[V](typeCastColumn: TypeCastColumn[_] with Reinterpretable)
      extends TypeCastColumn[V](typeCastColumn.targetColumn)

  //Tagging of compatible
  sealed trait Reinterpretable

  case class UInt8(tableColumn: ConstOrColMagnet[_], orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class UInt16(tableColumn: ConstOrColMagnet[_], orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class UInt32(tableColumn: ConstOrColMagnet[_], orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class UInt64(tableColumn: ConstOrColMagnet[_], orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable

  case class Int8(tableColumn: ConstOrColMagnet[_], orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class Int16(tableColumn: ConstOrColMagnet[_], orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class Int32(tableColumn: ConstOrColMagnet[_], orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class Int64(tableColumn: ConstOrColMagnet[_], orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable

  case class Float32(tableColumn: ConstOrColMagnet[_], orZero: Boolean = false)
      extends TypeCastColumn[Float](tableColumn)
      with Reinterpretable
  case class Float64(tableColumn: ConstOrColMagnet[_], orZero: Boolean = false)
      extends TypeCastColumn[Float](tableColumn)
      with Reinterpretable

  case class DateRep(tableColumn: ConstOrColMagnet[_])
      extends TypeCastColumn[org.joda.time.LocalDate](tableColumn)
      with Reinterpretable
  case class DateTimeRep(tableColumn: ConstOrColMagnet[_])
      extends TypeCastColumn[org.joda.time.DateTime](tableColumn)
      with Reinterpretable

  case class StringRep(tableColumn: ConstOrColMagnet[_])
      extends TypeCastColumn[String](tableColumn)
      with Reinterpretable
  case class FixedString(tableColumn: ConstOrColMagnet[_], n: Int) extends TypeCastColumn[String](tableColumn)
  case class StringCutToZero(tableColumn: ConstOrColMagnet[_])     extends TypeCastColumn[String](tableColumn)

  case class Cast[T](tableColumn: ConstOrColMagnet[_], simpleColumnType: SimpleColumnType)
      extends TypeCastColumn[T](tableColumn) with ConstOrColMagnet[T] {
    override val column: TableColumn[T] = this
  }

  sealed trait CastOutBind[I, O]
  implicit object UInt8CastOutBind       extends CastOutBind[ColumnType.UInt8.type, Int]
  implicit object UInt16CastOutBind      extends CastOutBind[ColumnType.UInt16.type, Int]
  implicit object UInt32CastOutBind      extends CastOutBind[ColumnType.UInt32.type, Int]
  implicit object UInt64CastOutBind      extends CastOutBind[ColumnType.UInt64.type, Int]
  implicit object Int8CastOutBind        extends CastOutBind[ColumnType.Int8.type, Int]
  implicit object Int16CastOutBind       extends CastOutBind[ColumnType.Int16.type, Int]
  implicit object Int32CastOutBind       extends CastOutBind[ColumnType.Int32.type, Int]
  implicit object Int64CastOutBind       extends CastOutBind[ColumnType.Int64.type, Int]
  implicit object Float32CastOutBind     extends CastOutBind[ColumnType.Float32.type, Float]
  implicit object Float64CastOutBind     extends CastOutBind[ColumnType.Float64.type, Float]
  implicit object StringCastOutBind      extends CastOutBind[ColumnType.String.type, Int]
  implicit object FixedStringCastOutBind extends CastOutBind[ColumnType.FixedString.type, Int]
  implicit object DateCastOutBind        extends CastOutBind[ColumnType.Date.type, Int]
  implicit object DateTimeCastOutBind    extends CastOutBind[ColumnType.DateTime.type, Int]

  def toUInt8[T: ClassTag](tableColumn: ConstOrColMagnet[T])  = UInt8(tableColumn)
  def toUInt16[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = UInt16(tableColumn)
  def toUInt32[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = UInt32(tableColumn)
  def toUInt64[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = UInt64(tableColumn)

  def toInt8[T: ClassTag](tableColumn: ConstOrColMagnet[T])  = Int8(tableColumn)
  def toInt16[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = Int16(tableColumn)
  def toInt32[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = Int32(tableColumn)
  def toInt64[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = Int64(tableColumn)

  def toFloat32[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = Float32(tableColumn)
  def toFloat64[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = Float64(tableColumn)

  def toUInt8OrZero[T: ClassTag](tableColumn: ConstOrColMagnet[T])  = UInt8(tableColumn, true)
  def toUInt16OrZero[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = UInt16(tableColumn, true)
  def toUInt32OrZero[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = UInt32(tableColumn, true)
  def toUInt64OrZero[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = UInt64(tableColumn, true)

  def toInt8OrZero[T: ClassTag](tableColumn: ConstOrColMagnet[T])  = Int8(tableColumn, true)
  def toInt16OrZero[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = Int16(tableColumn, true)
  def toInt32OrZero[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = Int32(tableColumn, true)
  def toInt64OrZero[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = Int64(tableColumn, true)

  def toFloat32OrZero[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = Float32(tableColumn, true)
  def toFloat64OrZero[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = Float64(tableColumn, true)

  def toDate[T: ClassTag](tableColumn: ConstOrColMagnet[T])     = DateRep(tableColumn)
  def toDateTime[T: ClassTag](tableColumn: ConstOrColMagnet[T]) = DateTimeRep(tableColumn)

  def toStringRep[T: ClassTag](tableColumn: ConstOrColMagnet[T])           = StringRep(tableColumn)
  def toFixedString[T: ClassTag](tableColumn: ConstOrColMagnet[T], n: Int) = FixedString(tableColumn, n)
  def toStringCutToZero[T: ClassTag](tableColumn: ConstOrColMagnet[T])     = StringCutToZero(tableColumn)

  def reinterpret[V](typeCastColumn: TypeCastColumn[_] with Reinterpretable) = Reinterpret[V](typeCastColumn)

  def cast[T <: SimpleColumnType, O](tableColumn: ConstOrColMagnet[_], simpleColumnType: T)(
      implicit castOut: CastOutBind[T, O]
  ) = Cast[O](tableColumn, simpleColumnType)
}
