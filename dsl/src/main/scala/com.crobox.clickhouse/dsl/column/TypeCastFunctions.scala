package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType.SimpleColumnType

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
      extends TypeCastColumn[T](tableColumn)

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

  def toUInt8(tableColumn: ConstOrColMagnet[_])  = UInt8(tableColumn)
  def toUInt16(tableColumn: ConstOrColMagnet[_]) = UInt16(tableColumn)
  def toUInt32(tableColumn: ConstOrColMagnet[_]) = UInt32(tableColumn)
  def toUInt64(tableColumn: ConstOrColMagnet[_]) = UInt64(tableColumn)

  def toInt8(tableColumn: ConstOrColMagnet[_])  = Int8(tableColumn)
  def toInt16(tableColumn: ConstOrColMagnet[_]) = Int16(tableColumn)
  def toInt32(tableColumn: ConstOrColMagnet[_]) = Int32(tableColumn)
  def toInt64(tableColumn: ConstOrColMagnet[_]) = Int64(tableColumn)

  def toFloat32(tableColumn: ConstOrColMagnet[_]) = Float32(tableColumn)
  def toFloat64(tableColumn: ConstOrColMagnet[_]) = Float64(tableColumn)

  def toUInt8OrZero(tableColumn: ConstOrColMagnet[_])  = UInt8(tableColumn, true)
  def toUInt16OrZero(tableColumn: ConstOrColMagnet[_]) = UInt16(tableColumn, true)
  def toUInt32OrZero(tableColumn: ConstOrColMagnet[_]) = UInt32(tableColumn, true)
  def toUInt64OrZero(tableColumn: ConstOrColMagnet[_]) = UInt64(tableColumn, true)

  def toInt8OrZero(tableColumn: ConstOrColMagnet[_])  = Int8(tableColumn, true)
  def toInt16OrZero(tableColumn: ConstOrColMagnet[_]) = Int16(tableColumn, true)
  def toInt32OrZero(tableColumn: ConstOrColMagnet[_]) = Int32(tableColumn, true)
  def toInt64OrZero(tableColumn: ConstOrColMagnet[_]) = Int64(tableColumn, true)

  def toFloat32OrZero(tableColumn: ConstOrColMagnet[_]) = Float32(tableColumn, true)
  def toFloat64OrZero(tableColumn: ConstOrColMagnet[_]) = Float64(tableColumn, true)

  def toDate(tableColumn: ConstOrColMagnet[_])     = DateRep(tableColumn)
  def toDateTime(tableColumn: ConstOrColMagnet[_]) = DateTimeRep(tableColumn)

  def toStringRep(tableColumn: ConstOrColMagnet[_])           = StringRep(tableColumn)
  def toFixedString(tableColumn: ConstOrColMagnet[_], n: Int) = FixedString(tableColumn, n)
  def toStringCutToZero(tableColumn: ConstOrColMagnet[_])     = StringCutToZero(tableColumn)

  def reinterpret[V](typeCastColumn: TypeCastColumn[_] with Reinterpretable) = Reinterpret[V](typeCastColumn)

  def cast[T <: SimpleColumnType, O](tableColumn: ConstOrColMagnet[_], simpleColumnType: T)(
      implicit castOut: CastOutBind[T, O]
  ) = Cast[O](tableColumn, simpleColumnType)
}
